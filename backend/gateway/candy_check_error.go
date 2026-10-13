package gateway

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"unicode"
)

// Persist only a bounded structured error message, not an HTML page or a raw
// request/response dump. Redact known credentials before truncating the text.
func (s *Service) candyCheckHTTPError(status int, body []byte, target *upstreamTarget, headers http.Header) string {
	reason := fmt.Sprintf("检测上游返回 HTTP %d", status)
	var payload map[string]any
	if len(body) > candyCheckBodyLimit || json.Unmarshal(body, &payload) != nil {
		return reason
	}
	message := s.walkErrorMessage(payload, 0)
	secrets := []string{}
	if target != nil {
		secrets = append(secrets, target.APIKey)
		if target.Provider != nil {
			var extra map[string]string
			if json.Unmarshal([]byte(target.Provider.ExtraHeadersJSON), &extra) == nil {
				for _, value := range extra {
					secrets = append(secrets, value)
				}
			}
		}
	}
	for name, values := range headers {
		if !s.isSensitiveHeader(name) {
			continue
		}
		for _, value := range values {
			secrets = append(secrets, value)
			if strings.EqualFold(name, "Authorization") || strings.EqualFold(name, "Proxy-Authorization") {
				if _, token, ok := strings.Cut(value, " "); ok {
					secrets = append(secrets, strings.TrimSpace(token))
				}
			}
			if strings.EqualFold(name, "Cookie") {
				for _, cookie := range strings.Split(value, ";") {
					if _, token, ok := strings.Cut(cookie, "="); ok {
						secrets = append(secrets, strings.Trim(strings.TrimSpace(token), "\""))
					}
				}
			}
		}
	}
	// Longest first avoids disclosing a suffix of overlapping credentials.
	sort.Slice(secrets, func(i, j int) bool { return len(secrets[i]) > len(secrets[j]) })
	for _, secret := range secrets {
		if secret != "" {
			message = strings.ReplaceAll(message, secret, "[redacted]")
		}
	}
	message = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, message)
	message = strings.Join(strings.Fields(message), " ")
	if message != "" {
		reason += ": " + s.truncateRunes(message, 240)
	}
	return reason
}
