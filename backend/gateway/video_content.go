package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"time"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/gin-gonic/gin"
)

// sub2api accepts a protected /videos/{id}/content relay URL, but rejects
// arbitrary third-party CDN URLs in its own video download handler.
func videoContentProxyBody(body []byte, taskID string) ([]byte, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(body, &object); err != nil {
		return nil, err
	}
	if videoJSONString(object["status"]) != "done" {
		return body, nil
	}
	var video map[string]json.RawMessage
	if err := json.Unmarshal(object["video"], &video); err != nil || video == nil {
		return nil, errors.New("completed video is missing")
	}
	video["url"], _ = json.Marshal("/v1/videos/" + url.PathEscape(taskID) + "/content")
	object["video"], _ = json.Marshal(video)
	return json.Marshal(object)
}

func (rt *Runtime) serveVideoTaskContent(c *gin.Context, target *upstreamTarget, taskBody []byte) {
	var task struct {
		Status string `json:"status"`
		Video  struct {
			URL string `json:"url"`
		} `json:"video"`
	}
	if json.Unmarshal(taskBody, &task) != nil || task.Status != "done" {
		rt.writeGatewayError(c, protocol.KindOpenAIChat, http.StatusConflict, "video_not_ready", "video task is not completed")
		return
	}
	if _, err := validateVideoContentURL(task.Video.URL); err != nil {
		rt.writeGatewayError(c, protocol.KindOpenAIChat, http.StatusBadGateway, "upstream_error", "video content URL is unavailable")
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Minute)
	defer cancel()
	release, err := rt.acquireUpstreamConcurrencyForAttempt(ctx, target, time.Now(), 0)
	if err != nil {
		rt.writeGatewayError(c, protocol.KindOpenAIChat, http.StatusServiceUnavailable, "upstream_error", "video download is unavailable")
		return
	}
	defer release()
	client := newVideoContentClient()
	defer client.CloseIdleConnections()
	if err := streamVideoContentMethod(ctx, c.Writer, client, task.Video.URL, c.Request.Method, c.GetHeader("Range"), c.GetHeader("If-Range")); err != nil && !c.Writer.Written() {
		for _, header := range []string{"Content-Type", "Content-Length", "Content-Range", "Content-Disposition", "ETag", "Last-Modified"} {
			c.Writer.Header().Del(header)
		}
		rt.writeGatewayError(c, protocol.KindOpenAIChat, http.StatusBadGateway, "upstream_error", "video download failed")
	}
}

func validateVideoContentURL(raw string) (*url.URL, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Hostname() == "" || parsed.User != nil || parsed.Fragment != "" ||
		(parsed.Scheme != "https" && parsed.Scheme != "http") {
		return nil, errors.New("invalid video content URL")
	}
	if port := parsed.Port(); port != "" && port != "443" && port != "80" {
		return nil, errors.New("unsupported video content port")
	}
	return parsed, nil
}

var videoContentBlockedNetworks = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("168.63.129.16/32"),
	netip.MustParsePrefix("2001::/32"),
	netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("2002::/16"),
	netip.MustParsePrefix("64:ff9b::/96"),
	netip.MustParsePrefix("64:ff9b:1::/48"),
}

func publicVideoContentIP(ip net.IP) bool {
	address, ok := netip.AddrFromSlice(ip)
	if !ok {
		return false
	}
	address = address.Unmap()
	if !address.IsGlobalUnicast() || address.IsPrivate() || address.IsLoopback() || address.IsLinkLocalUnicast() {
		return false
	}
	for _, prefix := range videoContentBlockedNetworks {
		if prefix.Contains(address) {
			return false
		}
	}
	return true
}

// Resolve and dial the validated IP in one operation. Validating a URL and
// then letting a second DNS lookup select the peer would permit rebinding.
func dialVideoContentAddress(ctx context.Context, network, address string,
	lookup func(context.Context, string) ([]net.IPAddr, error),
	dial func(context.Context, string, string) (net.Conn, error),
) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil || (port != "80" && port != "443") {
		return nil, errors.New("invalid video download address")
	}
	ips, err := lookup(ctx, host)
	if err != nil || len(ips) == 0 {
		return nil, errors.New("video download host cannot be resolved")
	}
	for _, ip := range ips {
		if ip.Zone != "" || !publicVideoContentIP(ip.IP) {
			return nil, errors.New("video download requires a public address")
		}
	}
	for _, ip := range ips {
		var conn net.Conn
		conn, err = dial(ctx, network, net.JoinHostPort(ip.IP.String(), port))
		if err == nil {
			return conn, nil
		}
	}
	return nil, err
}

func newVideoContentClient() *http.Client {
	dialer := &net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second}
	return &http.Client{
		Transport: &http.Transport{
			// Content URLs come from upstream responses, not admin configuration.
			// Do not forward API credentials or use a proxy that bypasses IP checks.
			DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
				return dialVideoContentAddress(ctx, network, address, net.DefaultResolver.LookupIPAddr, dialer.DialContext)
			},
			TLSHandshakeTimeout:   15 * time.Second,
			ResponseHeaderTimeout: 30 * time.Second,
			DisableCompression:    true,
			ForceAttemptHTTP2:     true,
		},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return errors.New("too many video redirects")
			}
			_, err := validateVideoContentURL(req.URL.String())
			return err
		},
	}
}

func streamVideoContent(ctx context.Context, dst http.ResponseWriter, client *http.Client, rawURL, byteRange string) error {
	return streamVideoContentMethod(ctx, dst, client, rawURL, http.MethodGet, byteRange, "")
}

func streamVideoContentMethod(ctx context.Context, dst http.ResponseWriter, client *http.Client, rawURL, method, byteRange, ifRange string) error {
	if _, err := validateVideoContentURL(rawURL); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, method, rawURL, nil)
	if err != nil {
		return err
	}
	if byteRange != "" {
		req.Header.Set("Range", byteRange)
		if ifRange != "" {
			req.Header.Set("If-Range", ifRange)
		}
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return copyVideoContentResponse(dst, resp, method)
}

func copyVideoContentResponse(dst http.ResponseWriter, resp *http.Response, method string) error {
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusRequestedRangeNotSatisfiable {
		return errors.New("video content upstream failed")
	}
	contentType := strings.ToLower(strings.TrimSpace(strings.Split(resp.Header.Get("Content-Type"), ";")[0]))
	if resp.StatusCode != http.StatusRequestedRangeNotSatisfiable && contentType != "" && !strings.HasPrefix(contentType, "video/") && contentType != "application/octet-stream" {
		return errors.New("upstream did not return video content")
	}
	for _, header := range []string{"Content-Type", "Content-Length", "Content-Range", "Accept-Ranges", "ETag", "Last-Modified"} {
		if value := resp.Header.Get(header); value != "" {
			dst.Header().Set(header, value)
		}
	}
	dst.Header().Set("Content-Disposition", `attachment; filename="video.mp4"`)
	dst.Header().Set("X-Content-Type-Options", "nosniff")
	dst.Header().Set("Cache-Control", "private, no-store")
	if contentType == "" {
		dst.Header().Set("Content-Type", "application/octet-stream")
	}
	if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		// Do not forward the upstream's JSON/HTML error body as a video.
		dst.Header().Set("Content-Length", "0")
		dst.WriteHeader(resp.StatusCode)
		return nil
	}
	dst.WriteHeader(resp.StatusCode)
	if method == http.MethodHead {
		return nil
	}
	_, err := io.Copy(dst, resp.Body)
	return err
}

// Native /videos tasks may complete without a URL. Their fixed content API
// belongs to the original account, so use the encrypted task credentials and
// the provider's proxy/concurrency settings, never a newly selected route.
func (rt *Runtime) serveNativeVideoContent(c *gin.Context, target *upstreamTarget, taskID string, kind protocolKind) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Minute)
	defer cancel()
	release, err := rt.acquireUpstreamConcurrencyForAttempt(ctx, target, time.Now(), 0)
	if err != nil {
		rt.writeVideoContentError(c, http.StatusServiceUnavailable, "video download is unavailable")
		return
	}
	defer release()
	req, err := rt.buildUpstreamHTTPRequest(gatewayAPICallContext(ctx, c), target,
		"/v1/videos/"+url.PathEscape(taskID)+"/content", c.Request.Method, c.Request.Header, nil, kind, false)
	if err != nil {
		rt.writeVideoContentError(c, http.StatusBadGateway, "video download is unavailable")
		return
	}
	req.Header.Set("Accept", "video/*, application/octet-stream")
	req.Header.Set("Accept-Encoding", "identity")
	if value := c.GetHeader("Range"); value != "" {
		req.Header.Set("Range", value)
		if value := c.GetHeader("If-Range"); value != "" {
			req.Header.Set("If-Range", value)
		}
	}
	client := rt.videoUpstreamClient(target)
	resp, err := client.Do(req)
	if err != nil {
		rt.writeVideoContentError(c, http.StatusBadGateway, "video download failed")
		return
	}
	defer resp.Body.Close()
	if isVideoContentRedirect(resp.StatusCode) {
		// Even a same-host CDN redirect gets a fresh credential-free client.
		// No upstream API key, custom auth header, cookie or proxy is carried.
		location, locationErr := resp.Location()
		if locationErr == nil {
			cdn := newVideoContentClient()
			defer cdn.CloseIdleConnections()
			err = streamVideoContentMethod(ctx, c.Writer, cdn, location.String(), c.Request.Method, c.GetHeader("Range"), c.GetHeader("If-Range"))
		} else {
			err = locationErr
		}
	} else {
		err = copyVideoContentResponse(c.Writer, resp, c.Request.Method)
	}
	if err != nil && !c.Writer.Written() {
		status := http.StatusBadGateway
		switch resp.StatusCode {
		case http.StatusNotFound, http.StatusConflict, http.StatusGone:
			status = resp.StatusCode
		}
		rt.writeVideoContentError(c, status, "video content is unavailable or not ready")
	}
}

func isVideoContentRedirect(status int) bool {
	return status == http.StatusMovedPermanently || status == http.StatusFound ||
		status == http.StatusSeeOther || status == http.StatusTemporaryRedirect || status == http.StatusPermanentRedirect
}

func (rt *Runtime) videoUpstreamClient(target *upstreamTarget) *http.Client {
	client := *rt.httpClientForTarget(target.Channel, target.Provider)
	client.Timeout = 10 * time.Minute
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &client
}

func (rt *Runtime) writeVideoContentError(c *gin.Context, status int, message string) {
	if c.Request.Method == http.MethodHead {
		c.Status(status)
		return
	}
	rt.writeGatewayError(c, protocol.KindOpenAIChat, status, "upstream_error", message)
}
