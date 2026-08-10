package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/gin-gonic/gin"
)

// A missing stream usage field should not make a normal request wait for the
// full gateway forwarding timeout. The count endpoint is a small follow-up
// request and gets its own bounded budget.
const streamInputUsageFallbackTimeout = 5 * time.Second

// prepareAnthropicCountTokensBody removes generation-only stream controls that
// are not valid options for the Anthropic count_tokens endpoint.
func prepareAnthropicCountTokensBody(body []byte) ([]byte, error) {
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("invalid Anthropic request body: %w", err)
	}
	if payload == nil {
		return nil, fmt.Errorf("Anthropic request body must be a JSON object")
	}
	delete(payload, "stream")
	delete(payload, "stream_options")
	return json.Marshal(payload)
}

func (rt *Runtime) countAnthropicInputTokens(
	ctx context.Context,
	c *gin.Context,
	target *upstreamTarget,
	body []byte,
) (int, error) {
	if rt == nil || target == nil {
		return 0, fmt.Errorf("missing runtime or upstream target")
	}
	countBody, err := prepareAnthropicCountTokensBody(body)
	if err != nil {
		return 0, err
	}
	if ctx == nil {
		ctx = context.Background()
	} else {
		// A committed stream is deliberately drained after client cancellation;
		// the follow-up count must retain that same billing behavior.
		ctx = context.WithoutCancel(ctx)
	}
	timeout := streamInputUsageFallbackTimeout
	if configured := rt.gatewayRuntime().ForwardTimeout(); configured > 0 && configured < timeout {
		timeout = configured
	}
	countCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var headers http.Header
	if c != nil && c.Request != nil {
		headers = c.Request.Header
	}
	status, _, responseBody, _, forwardErr := rt.forwardOnce(
		countCtx, c, target, "/v1/messages/count_tokens", http.MethodPost,
		headers, countBody, false, protocol.KindAnthropic, timeout,
	)
	if forwardErr != nil {
		return 0, forwardErr
	}
	if status < 200 || status >= 300 {
		return 0, fmt.Errorf("count_tokens returned HTTP %d", status)
	}
	counted := ParseOpenAIUsage(responseBody).InputTokens
	if counted <= 0 {
		return 0, fmt.Errorf("count_tokens response did not contain a positive input_tokens value")
	}
	return counted, nil
}

// recoverMissingStreamInputTokens repairs providers that return output usage
// on an Anthropic stream but omit input_tokens. Callers invoke it only after
// the stream has released its provider/channel concurrency slot.
func (rt *Runtime) recoverMissingStreamInputTokens(
	ctx context.Context,
	c *gin.Context,
	target *upstreamTarget,
	body []byte,
	upstreamKind protocolKind,
	tokens UsageTokens,
) UsageTokens {
	if tokens.InputTokens > 0 || protocol.NormalizeKind(upstreamKind) != protocol.KindAnthropic {
		return tokens
	}
	counted, err := rt.countAnthropicInputTokens(ctx, c, target, body)
	if err != nil {
		if rt != nil && rt.Log != nil {
			rt.Log.Warn("recover missing stream input tokens failed", "provider_id", streamUsageProviderID(target), "err", err)
		}
		return tokens
	}

	// count_tokens returns the total request size, whereas Messages usage keeps
	// cache reads/creates in separate billing buckets. Subtract known cache
	// buckets when the response is consistent with a total. Some compatible
	// providers return fresh input directly; an inconsistent smaller count is
	// therefore retained as-is.
	knownCache := tokens.CacheReadTokens + tokens.CacheCreationTokens
	if knownCache == 0 {
		knownCache = tokens.CacheCreation5mTokens + tokens.CacheCreation1hTokens
	}
	if knownCache > 0 && counted >= knownCache {
		counted -= knownCache
	}
	if counted > 0 {
		tokens.InputTokens = counted
	}
	return tokens
}

func streamUsageProviderID(target *upstreamTarget) uint {
	if target != nil && target.Provider != nil {
		return target.Provider.ID
	}
	return 0
}

// rewriteAnthropicStreamUsageInput fills only missing input_tokens fields in
// an Anthropic SSE payload. Usage may be nested under message.usage or exposed
// at the event root; compatible providers use both shapes.
func rewriteAnthropicStreamUsageInput(data string, input int) (string, bool) {
	if strings.TrimSpace(data) == "" || input <= 0 {
		return data, false
	}
	var root any
	if err := json.Unmarshal([]byte(data), &root); err != nil {
		return data, false
	}
	changed := false
	var visit func(any)
	visit = func(value any) {
		switch node := value.(type) {
		case map[string]any:
			if usage, ok := node["usage"].(map[string]any); ok {
				if _, exists := usage["input_tokens"]; !exists || mapInt(usage, "input_tokens") <= 0 {
					usage["input_tokens"] = input
					changed = true
				}
			}
			for _, child := range node {
				visit(child)
			}
		case []any:
			for _, child := range node {
				visit(child)
			}
		}
	}
	visit(root)
	if !changed {
		return data, false
	}
	rewritten, err := json.Marshal(root)
	if err != nil {
		return data, false
	}
	return string(rewritten), true
}

// replaceSSEEventData keeps event/id/retry lines intact while replacing the
// JSON data payload after a recovered usage count has been applied.
func replaceSSEEventData(lines []string, data string) []string {
	if len(lines) == 0 {
		return lines
	}
	out := make([]string, 0, len(lines)+1)
	inserted := false
	for _, line := range lines {
		if strings.HasPrefix(line, "data:") {
			if !inserted {
				for _, part := range strings.Split(data, "\n") {
					out = append(out, "data: "+part)
				}
				inserted = true
			}
			continue
		}
		out = append(out, line)
	}
	if !inserted {
		out = append(out, "data: "+data)
	}
	return out
}
