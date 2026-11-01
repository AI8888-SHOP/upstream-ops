// Package apiaudit records actual outbound HTTP attempts without changing customer billing.
package apiaudit

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
)

const captureLimit = 2 << 20

type contextKey struct{}
type attemptKey struct{}
type attemptID struct {
	id   string
	used atomic.Bool
}

// WithMetadata carries attribution only. It never carries credentials or prompts.
func WithMetadata(ctx context.Context, meta storage.GatewayAPICallLog) context.Context {
	prior, _ := ctx.Value(contextKey{}).(storage.GatewayAPICallLog)
	mergeMetadata(&prior, meta)
	ctx = context.WithValue(ctx, contextKey{}, prior)
	if meta.RequestID != "" {
		ctx = context.WithValue(ctx, attemptKey{}, &attemptID{id: meta.RequestID})
	}
	return ctx
}
func mergeMetadata(a *storage.GatewayAPICallLog, b storage.GatewayAPICallLog) {
	if b.Purpose != "" {
		a.Purpose = b.Purpose
	}
	if b.RequestID != "" {
		a.RequestID = b.RequestID
	}
	if b.GatewayRequestID != "" {
		a.GatewayRequestID = b.GatewayRequestID
	}
	if b.Model != "" {
		a.Model = b.Model
	}
	if b.GatewayGroupID != 0 {
		a.GatewayGroupID = b.GatewayGroupID
	}
	if b.RouteID != 0 {
		a.RouteID = b.RouteID
	}
	if b.ChannelID != 0 {
		a.ChannelID = b.ChannelID
	}
	if b.GatewayProviderID != 0 {
		a.GatewayProviderID = b.GatewayProviderID
	}
	if b.SourceGroupID != nil {
		v := *b.SourceGroupID
		a.SourceGroupID = &v
	}
	if b.SourceAPIKeyID != 0 {
		a.SourceAPIKeyID = b.SourceAPIKeyID
	}
	if b.SourceAPIKeyName != "" {
		a.SourceAPIKeyName = b.SourceAPIKeyName
	}
}
func NewRequestID() string {
	var data [16]byte
	if _, err := rand.Read(data[:]); err != nil {
		panic("api audit random source failed")
	}
	return "uops_" + hex.EncodeToString(data[:])
}

// UsageSnapshot carries raw protocol buckets until the final normalization.
type UsageSnapshot struct {
	InputTokens, OutputTokens, CacheReadTokens, CacheCreationTokens                  int
	CacheCreation5mTokens, CacheCreation1hTokens, ImageOutputTokens, ReasoningTokens int
	InputSeen, OutputSeen                                                            bool
	Known                                                                            bool
}

type Options struct {
	Store    *storage.GatewayAPICallLogs
	Purpose  string
	Metadata storage.GatewayAPICallLog
	Log      *slog.Logger
	// OnFinish can normalize protocol usage and attach an estimated operator cost.
	OnFinish   func(*storage.GatewayAPICallLog)
	ParseUsage func([]byte, string, UsageSnapshot) UsageSnapshot
}
type transport struct {
	base http.RoundTripper
	opts Options
}

func WrapTransport(base http.RoundTripper, opts Options) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	if opts.Store == nil {
		return base
	}
	return &transport{base: base, opts: opts}
}
func (t *transport) RoundTrip(req *http.Request) (*http.Response, error) {
	item := t.opts.Metadata
	if item.Purpose == "" {
		item.Purpose = t.opts.Purpose
	}
	if meta, ok := req.Context().Value(contextKey{}).(storage.GatewayAPICallLog); ok {
		mergeMetadata(&item, meta)
	}
	// The same request can be redirected/retried: every RoundTrip owns a new row.
	// A caller-supplied ID belongs to one attempt only (e.g. a candy probe).
	item.RequestID = NewRequestID()
	if a, ok := req.Context().Value(attemptKey{}).(*attemptID); ok && a.used.CompareAndSwap(false, true) {
		item.RequestID = a.id
	}
	item.ID = 0
	item.Method = req.Method
	item.Host = req.URL.Host
	item.Endpoint = redactPath(req.URL.EscapedPath())
	item.StartedAt = time.Now().UTC()
	item.CreatedAt = item.StartedAt
	item.Status = storage.GatewayAPICallStatusPending
	item.FinishedAt = nil
	item.UsageKnown = false
	item.EstimatedCost = nil
	if req.GetBody != nil {
		if r, e := req.GetBody(); e == nil {
			data, _ := io.ReadAll(io.LimitReader(r, captureLimit))
			_ = r.Close()
			var m struct {
				Model  string `json:"model"`
				Stream bool   `json:"stream"`
			}
			if json.Unmarshal(data, &m) == nil {
				if item.Model == "" {
					item.Model = m.Model
				}
				item.Stream = m.Stream
			}
		}
	}
	if err := t.opts.Store.Create(&item); err != nil {
		if req.Body != nil {
			_ = req.Body.Close()
		}
		if t.opts.Log != nil {
			t.opts.Log.Error("upstream call blocked: audit insert failed", "purpose", item.Purpose)
		}
		return nil, errors.New("upstream API audit storage failed before request")
	}
	call := &callState{opts: t.opts, item: item}
	if strings.Contains(req.URL.Path, "/messages") {
		call.protocol = "anthropic"
	}
	resp, err := t.base.RoundTrip(req)
	if err != nil {
		call.finish("transport", false)
		return resp, err
	}
	if resp == nil {
		call.finish("transport", false)
		return nil, errors.New("upstream returned no response")
	}
	call.item.StatusCode = resp.StatusCode
	for _, k := range []string{"X-Request-Id", "Request-Id", "X-Upstream-Ops-Request-Id"} {
		if v := resp.Header.Get(k); v != "" {
			call.item.UpstreamRequestID = v
			break
		}
	}
	call.sse = strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "text/event-stream")
	call.item.Stream = call.item.Stream || call.sse
	if resp.Body == nil {
		call.finish("", true)
		return resp, nil
	}
	resp.Body = &body{ReadCloser: resp.Body, call: call}
	return resp, nil
}

type callState struct {
	mu         sync.Mutex
	once       sync.Once
	opts       Options
	item       storage.GatewayAPICallLog
	sse        bool
	jsonBuffer []byte
	line       []byte
	event      []byte
	dropping   bool
	terminal   bool
	failed     bool
	protocol   string
	snapshot   UsageSnapshot
	finished   bool
}
type body struct {
	io.ReadCloser
	call *callState
}

func (b *body) Read(p []byte) (int, error) {
	n, e := b.ReadCloser.Read(p)
	if n > 0 {
		b.call.consume(p[:n])
	}
	if e != nil {
		if errors.Is(e, io.EOF) {
			b.call.finish("", true)
		} else {
			b.call.finish("body_read", false)
		}
	}
	return n, e
}
func (b *body) Close() error {
	err := b.ReadCloser.Close()
	kind := ""
	if err != nil {
		kind = "body_close"
	}
	b.call.finish(kind, false)
	return err
}
func (c *callState) consume(p []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.finished {
		return
	}
	if !c.sse {
		if len(c.jsonBuffer)+len(p) <= captureLimit {
			c.jsonBuffer = append(c.jsonBuffer, p...)
		} else {
			c.dropping = true
			c.jsonBuffer = nil
		}
		return
	}
	for _, ch := range p {
		if ch == '\n' {
			c.lineComplete()
			continue
		}
		if !c.dropping {
			if len(c.line) < captureLimit {
				c.line = append(c.line, ch)
			} else {
				c.dropping = true
				c.line = nil
				c.event = nil
			}
		}
	}
}
func (c *callState) lineComplete() {
	line := bytes.TrimSuffix(c.line, []byte{'\r'})
	c.line = nil
	if len(line) == 0 {
		if !c.dropping && len(c.event) > 0 {
			c.parsePayload(bytes.TrimSpace(c.event))
		}
		c.event = nil
		c.dropping = false
		return
	}
	if c.dropping {
		return
	}
	if bytes.HasPrefix(line, []byte("data:")) {
		v := bytes.TrimPrefix(line, []byte("data:"))
		if len(c.event)+len(v) > captureLimit {
			c.dropping = true
			c.event = nil
			return
		}
		c.event = append(c.event, v...)
		c.event = append(c.event, '\n')
	}
}
func (c *callState) finish(kind string, eof bool) {
	c.once.Do(func() {
		c.mu.Lock()
		if c.sse {
			if len(c.line) > 0 {
				c.lineComplete()
			}
			if len(c.event) > 0 && !c.dropping {
				c.parsePayload(bytes.TrimSpace(c.event))
			}
		} else if !c.dropping {
			c.parsePayload(c.jsonBuffer)
		}
		c.item.Status = storage.GatewayAPICallStatusSuccess
		if kind != "" || c.item.StatusCode < 200 || c.item.StatusCode >= 300 || c.failed {
			c.item.Status = storage.GatewayAPICallStatusError
			if kind == "" {
				if c.failed {
					kind = "upstream_error"
				} else {
					kind = "http_status"
				}
			}
		} else if (c.sse || c.item.Stream) && !c.terminal {
			c.item.Status = storage.GatewayAPICallStatusIncomplete
			kind = "stream_incomplete"
		} else if !eof && !c.terminal {
			c.item.Status = storage.GatewayAPICallStatusIncomplete
			kind = "body_not_consumed"
		}
		if c.item.Status == storage.GatewayAPICallStatusIncomplete || kind == "body_read" || kind == "body_close" {
			c.item.UsageKnown = false
		}
		c.item.ErrorType = kind
		switch kind {
		case "transport":
			c.item.ErrorMessage = "upstream transport failed"
		case "body_read", "body_close":
			c.item.ErrorMessage = "upstream response body interrupted"
		case "http_status":
			c.item.ErrorMessage = http.StatusText(c.item.StatusCode)
		case "upstream_error":
			c.item.ErrorMessage = "upstream returned an error event"
		case "stream_incomplete":
			c.item.ErrorMessage = "response stream ended without a terminal event"
		case "body_not_consumed":
			c.item.ErrorMessage = "response closed before completion"
		}
		now := time.Now().UTC()
		c.item.FinishedAt = &now
		c.item.DurationMS = now.Sub(c.item.StartedAt).Milliseconds()
		c.finished = true
		// Anthropic input is already exclusive; OpenAI cache counters are part of input.
		if c.protocol != "anthropic" {
			c.item.InputTokens = max(0, c.item.InputTokens-c.item.CacheReadTokens-c.item.CacheCreationTokens)
		}
		c.mu.Unlock()
		if c.opts.OnFinish != nil {
			c.opts.OnFinish(&c.item)
		}
		if err := c.opts.Store.SaveFinish(&c.item); err != nil && c.opts.Log != nil {
			c.opts.Log.Error("upstream API audit finalization failed", "request_id", c.item.RequestID, "purpose", c.item.Purpose)
		}
	})
}
func (c *callState) parsePayload(p []byte) {
	if bytes.Equal(bytes.TrimSpace(p), []byte("[DONE]")) {
		c.terminal = true
		return
	}
	var raw map[string]json.RawMessage
	if json.Unmarshal(p, &raw) != nil {
		return
	}
	var typ, status string
	_ = json.Unmarshal(raw["type"], &typ)
	_ = json.Unmarshal(raw["status"], &status)
	if status == "completed" {
		c.terminal = true
	}
	if status == "failed" || status == "incomplete" {
		c.failed = true
		c.terminal = true
	}
	if typ == "message" {
		c.protocol = "anthropic"
	}
	if c.sse && c.item.FirstTokenMS == nil && payloadHasContent(raw) {
		n := time.Since(c.item.StartedAt).Milliseconds()
		c.item.FirstTokenMS = &n
	}
	switch typ {
	case "error", "response.failed", "response.error", "response.incomplete":
		c.failed = true
		c.terminal = true
	case "response.completed", "response.done", "message_stop":
		c.terminal = true
	}
	if e := raw["error"]; len(e) > 0 && string(e) != "null" {
		c.failed = true
	}
	usage := raw["usage"]
	for _, key := range []string{"response", "message", "data"} {
		if nested := raw[key]; len(nested) > 0 {
			var obj map[string]json.RawMessage
			if json.Unmarshal(nested, &obj) == nil {
				if len(obj["usage"]) > 0 {
					usage = obj["usage"]
				}
				var status string
				_ = json.Unmarshal(obj["status"], &status)
				if status == "failed" || status == "incomplete" {
					c.failed = true
				}
			}
		}
	}
	if typ == "message_start" || typ == "message_delta" || typ == "message_stop" {
		c.protocol = "anthropic"
	}
	var choices []struct {
		FinishReason *string `json:"finish_reason"`
	}
	if json.Unmarshal(raw["choices"], &choices) == nil {
		for _, v := range choices {
			if v.FinishReason != nil && *v.FinishReason != "" {
				c.terminal = true
			}
		}
	}
	if c.item.Purpose == storage.GatewayAPICallPurposeChannelAPI || c.item.Purpose == storage.GatewayAPICallPurposeSyncAPI {
		return
	}
	if c.opts.ParseUsage != nil {
		c.snapshot = c.opts.ParseUsage(p, c.protocol, c.snapshot)
		u := c.snapshot
		c.item.InputTokens = u.InputTokens
		c.item.OutputTokens = u.OutputTokens
		c.item.CacheReadTokens = u.CacheReadTokens
		c.item.CacheCreationTokens = u.CacheCreationTokens
		c.item.CacheCreation5mTokens = u.CacheCreation5mTokens
		c.item.CacheCreation1hTokens = u.CacheCreation1hTokens
		c.item.ImageOutputTokens = u.ImageOutputTokens
		c.item.ReasoningTokens = u.ReasoningTokens
		c.item.UsageKnown = u.Known
		return
	}
	var u map[string]json.RawMessage
	if json.Unmarshal(usage, &u) != nil || u == nil {
		return
	}
	known := false
	readInt := func(m map[string]json.RawMessage, keys ...string) (int, bool) {
		for _, k := range keys {
			v, ok := m[k]
			if !ok {
				continue
			}
			var n json.Number
			if json.Unmarshal(v, &n) == nil {
				if i, e := n.Int64(); e == nil && i >= 0 && i <= int64(^uint(0)>>1) {
					return int(i), true
				}
			}
		}
		return 0, false
	}
	if v, ok := readInt(u, "input_tokens", "prompt_tokens"); ok {
		c.item.InputTokens = v
		known = true
	}
	if v, ok := readInt(u, "output_tokens", "completion_tokens"); ok {
		c.item.OutputTokens = v
		known = true
	}
	if v, ok := readInt(u, "cache_read_input_tokens", "cache_read_tokens", "cached_tokens", "prompt_cache_hit_tokens"); ok {
		c.item.CacheReadTokens = v
		known = true
	}
	if v, ok := readInt(u, "cache_creation_input_tokens", "cache_creation_tokens"); ok {
		c.item.CacheCreationTokens = v
		known = true
	}
	for _, k := range []string{"input_tokens_details", "prompt_tokens_details"} {
		var details map[string]json.RawMessage
		if json.Unmarshal(u[k], &details) == nil {
			if v, ok := readInt(details, "cached_tokens"); ok {
				c.item.CacheReadTokens = v
				known = true
			}
		}
	}
	c.item.UsageKnown = c.item.UsageKnown || known
}

// Secret-bearing resource URLs are recorded as templates, never as credentials.
func redactPath(path string) string {
	parts := strings.Split(path, "/")
	secretNext := false
	for i, v := range parts {
		decoded, _ := url.PathUnescape(v)
		lower := strings.ToLower(decoded)
		if secretNext || strings.HasPrefix(lower, "sk-") || len(decoded) > 96 {
			parts[i] = "[redacted]"
			secretNext = false
			continue
		}
		switch lower {
		case "token", "subscription", "subscribe", "secret", "password":
			secretNext = true
		}
	}
	return strings.Join(parts, "/")
}

func payloadHasContent(raw map[string]json.RawMessage) bool {
	var typ string
	_ = json.Unmarshal(raw["type"], &typ)
	if typ == "response.output_text.delta" || typ == "content_block_delta" {
		return len(raw["delta"]) > 0 && string(raw["delta"]) != `""`
	}
	for _, k := range []string{"choices", "output", "content"} {
		var list []map[string]json.RawMessage
		if json.Unmarshal(raw[k], &list) != nil {
			continue
		}
		for _, v := range list {
			if len(v["text"]) > 2 || len(v["content"]) > 2 || len(v["tool_calls"]) > 2 {
				return true
			}
			for _, nested := range []string{"delta", "message"} {
				var m map[string]json.RawMessage
				if json.Unmarshal(v[nested], &m) == nil {
					for _, key := range []string{"content", "tool_calls", "reasoning_content"} {
						if len(m[key]) > 2 && string(m[key]) != "null" {
							return true
						}
					}
				}
			}
		}
	}
	return false
}
