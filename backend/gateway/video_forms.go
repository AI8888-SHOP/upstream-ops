package gateway

import (
	"bytes"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"net/url"
	"strings"
)

const videoRequestBodyLimit int64 = 256 << 20

// Native video requests accept JSON, URL-encoded forms and multipart files.
// Read only small routing fields; never decode binary references as JSON.
func videoFormFields(contentType string, body []byte) (url.Values, error) {
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		if strings.TrimSpace(contentType) == "" {
			return nil, nil
		}
		return nil, errors.New("invalid video Content-Type")
	}
	var fields url.Values
	switch mediaType {
	case "application/x-www-form-urlencoded":
		fields, err = url.ParseQuery(string(body))
		if err != nil {
			return nil, errors.New("invalid video form")
		}
	case "multipart/form-data":
		fields = make(url.Values)
		reader := multipart.NewReader(bytes.NewReader(body), params["boundary"])
		for count := 0; ; count++ {
			part, err := reader.NextRawPart()
			if err == io.EOF {
				break
			}
			if err != nil || count >= 128 {
				return nil, errors.New("invalid video multipart form")
			}
			name := part.FormName()
			if name == "model" || name == "stream" {
				value, err := io.ReadAll(io.LimitReader(part, 1025))
				if err != nil || len(value) > 1024 || part.FileName() != "" {
					return nil, errors.New("invalid video model or stream field")
				}
				fields.Add(name, string(value))
			}
			if err := part.Close(); err != nil {
				return nil, errors.New("invalid video multipart form")
			}
		}
	default:
		return nil, nil
	}
	if len(fields["model"]) > 1 || len(fields["stream"]) > 1 {
		return nil, errors.New("duplicate video model or stream field")
	}
	return fields, nil
}

// Use the original boundary so the caller's Content-Type remains correct.
// Each attempt rewrites its own body; failover must not inherit another
// route's model mapping or alter reference file bytes and metadata.
func rewriteVideoFormModel(contentType string, body []byte, model string) ([]byte, error) {
	fields, err := videoFormFields(contentType, body)
	if err != nil || fields == nil || fields.Get("model") == model || model == "" {
		return body, err
	}
	mediaType, params, _ := mime.ParseMediaType(contentType)
	if mediaType == "application/x-www-form-urlencoded" {
		fields.Set("model", model)
		return []byte(fields.Encode()), nil
	}
	var out bytes.Buffer
	writer := multipart.NewWriter(&out)
	if err := writer.SetBoundary(params["boundary"]); err != nil {
		return nil, err
	}
	reader := multipart.NewReader(bytes.NewReader(body), params["boundary"])
	for {
		part, err := reader.NextRawPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		header := part.Header
		if part.FormName() == "model" {
			header.Del("Content-Length")
			header.Del("Content-Transfer-Encoding")
		}
		dst, err := writer.CreatePart(header)
		if err != nil {
			return nil, err
		}
		if part.FormName() == "model" {
			_, err = io.WriteString(dst, model)
		} else {
			_, err = io.Copy(dst, part)
		}
		_ = part.Close()
		if err != nil {
			return nil, err
		}
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}
