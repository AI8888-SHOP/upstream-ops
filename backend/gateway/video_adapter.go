package gateway

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

const (
	videoModePassthrough = "passthrough"
	videoModeVideos      = "videos"
	videoResponseGrok    = "grok"
)

func normalizeVideoUpstreamMode(value string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", videoModePassthrough:
		return videoModePassthrough, nil
	case videoModeVideos:
		return videoModeVideos, nil
	default:
		return "", errors.New("video_upstream_mode must be passthrough or videos")
	}
}

func usesVideosAdapter(target *upstreamTarget, path string) bool {
	return strings.TrimPrefix(path, "/v1") == "/videos/generations" &&
		target != nil && target.Provider != nil && target.Provider.VideoUpstreamMode == videoModeVideos
}

// Run after model mapping, outside the shared protocol/body cache: two
// providers with the same text protocol may use different video APIs.
func prepareVideoUpstreamRequest(target *upstreamTarget, path string, body []byte, upstreamPath string) ([]byte, string, bool, error) {
	if !usesVideosAdapter(target, path) {
		return body, upstreamPath, false, nil
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(body, &object); err != nil || object == nil {
		return nil, "", false, errors.New("/v1/videos adaptation requires a JSON object")
	}
	// xAI/sub2api uses image:{url:...}; Image2Pro's common video contract
	// uses input_image or images. Explicit reference_images (Veo/Omni) and
	// provider-specific parameters stay intact; do not infer them from model names.
	if raw, exists := object["image"]; exists {
		for _, field := range []string{"input_image", "images", "reference_images"} {
			if _, exists := object[field]; exists {
				return nil, "", false, fmt.Errorf("image and %s cannot be combined in /v1/videos adaptation", field)
			}
		}
		imageURL, err := videoReferenceURL(raw)
		if err != nil {
			return nil, "", false, err
		}
		object["input_image"], _ = json.Marshal(imageURL)
		delete(object, "image")
	}
	if raw, exists := object["images"]; exists {
		var items []json.RawMessage
		if err := json.Unmarshal(raw, &items); err != nil {
			return nil, "", false, errors.New("video images must be an array")
		}
		images := make([]string, 0, len(items))
		for _, item := range items {
			imageURL, err := videoReferenceURL(item)
			if err != nil {
				return nil, "", false, err
			}
			images = append(images, imageURL)
		}
		object["images"], _ = json.Marshal(images)
	}
	// Video creation is asynchronous; do not send text stream options.
	delete(object, "stream")
	delete(object, "stream_options")
	encoded, err := json.Marshal(object)
	return encoded, "/v1/videos", true, err
}

func videoReferenceURL(raw json.RawMessage) (string, error) {
	if value := videoJSONString(raw); value != "" {
		return value, nil
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(raw, &object) == nil {
		for _, field := range []string{"url", "image_url"} {
			if value := videoJSONString(object[field]); value != "" {
				return value, nil
			}
		}
	}
	return "", errors.New("video image must be a URL, data URL, or an object with url")
}

func videoJSONString(raw json.RawMessage) string {
	var value string
	_ = json.Unmarshal(raw, &value)
	return strings.TrimSpace(value)
}

func videoPositiveNumber(raw json.RawMessage) float64 {
	var number float64
	if err := json.Unmarshal(raw, &number); err != nil {
		number, _ = strconv.ParseFloat(videoJSONString(raw), 64)
	}
	if number <= 0 || math.IsNaN(number) || math.IsInf(number, 0) {
		return 0
	}
	return number
}

func videoRequestDuration(body []byte) float64 {
	var object map[string]json.RawMessage
	if json.Unmarshal(body, &object) != nil {
		return 0
	}
	for _, field := range []string{"duration", "seconds"} {
		if value := videoPositiveNumber(object[field]); value > 0 {
			return value
		}
	}
	return 0
}

func videoCreateClientBody(target *upstreamTarget, path string, body []byte) ([]byte, error) {
	if !usesVideosAdapter(target, path) {
		return body, nil
	}
	taskID, err := videoTaskIDFromResponse(body)
	if err != nil {
		return nil, err
	}
	if taskID == "" {
		return nil, errors.New("/v1/videos creation returned no task ID")
	}
	return json.Marshal(map[string]string{"request_id": taskID})
}

// Convert only bound tasks that were created using the adapter. Never guess
// a format from the provider's current configuration after an admin edit.
func videoLookupClientBody(body []byte, taskID string, snapshot videoTaskSnapshot) ([]byte, error) {
	if snapshot.ResponseFormat != videoResponseGrok {
		return body, nil
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(body, &object); err != nil || object == nil {
		return nil, errors.New("invalid video task response")
	}
	var nested map[string]json.RawMessage
	_ = json.Unmarshal(object["data"], &nested)
	// Status responses may omit the ID. When supplied, every identifier
	// must still name the task we actually queried.
	for _, candidate := range []map[string]json.RawMessage{object, nested} {
		for _, field := range []string{"id", "request_id", "task_id"} {
			if raw, exists := candidate[field]; exists {
				id := videoJSONString(raw)
				if !validVideoTaskID(id) || id != taskID {
					return nil, errors.New("video task response ID does not match the bound task")
				}
			}
		}
	}
	if videoJSONString(object["status"]) == "" {
		if nested != nil {
			object = nested
		}
	}
	status := strings.ToLower(videoJSONString(object["status"]))
	switch status {
	case "queued", "processing", "in_progress", "running", "pending":
		status = "pending"
	case "completed", "succeeded", "success", "done":
		status = "done"
	case "failed", "error", "canceled", "cancelled":
		status = "failed"
	case "expired":
		status = "expired"
	default:
		return nil, errors.New("unknown video task status")
	}
	result := map[string]any{"request_id": taskID, "status": status}
	if snapshot.RequestedModel != "" {
		// The public model alias belongs to the downstream billing contract.
		result["model"] = snapshot.RequestedModel
	}
	for _, field := range []string{"progress", "poll_after_ms", "poll_interval_ms"} {
		if raw, exists := object[field]; exists {
			result[field] = raw
		}
	}
	if status == "failed" || status == "expired" {
		if raw, exists := object["error"]; exists && string(raw) != "null" {
			if message := videoJSONString(raw); message != "" {
				result["error"] = map[string]string{"message": message}
			} else {
				result["error"] = raw
			}
		}
	}
	if status == "done" {
		var video, metadata map[string]json.RawMessage
		var output []map[string]json.RawMessage
		_ = json.Unmarshal(object["video"], &video)
		_ = json.Unmarshal(object["metadata"], &metadata)
		_ = json.Unmarshal(object["output"], &output)
		sources := []map[string]json.RawMessage{video, object, metadata}
		if len(output) > 0 {
			sources = append(sources, output[0])
		}
		videoURL := ""
		duration := float64(0)
		for _, source := range sources {
			if videoURL == "" {
				videoURL = videoJSONString(source["url"])
			}
			for _, field := range []string{"duration", "seconds"} {
				if duration == 0 {
					duration = videoPositiveNumber(source[field])
				}
			}
		}
		parsed, err := url.Parse(videoURL)
		if err != nil || parsed.Host == "" || (parsed.Scheme != "https" && parsed.Scheme != "http") {
			return nil, errors.New("completed video task has no valid video URL")
		}
		if duration == 0 {
			duration = snapshot.DurationSeconds
		}
		completed := map[string]any{"url": videoURL}
		if duration > 0 {
			completed["duration"] = duration
		}
		if raw, exists := video["respect_moderation"]; exists {
			completed["respect_moderation"] = raw
		}
		result["video"] = completed
	}
	return json.Marshal(result)
}

func adaptedVideoHeaders(headers http.Header, taskID string) http.Header {
	out := headers.Clone()
	if out == nil {
		out = make(http.Header)
	}
	for _, field := range []string{"Content-Length", "Content-MD5", "ETag", "Location"} {
		out.Del(field)
	}
	out.Set("Content-Type", "application/json")
	if taskID != "" {
		out.Set("Location", "/v1/videos/"+taskID)
	}
	return out
}
