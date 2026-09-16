package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestExecutableUpdatesRequireAuthenticationEnabled(t *testing.T) {
	router := gin.New()
	registerUpdates(router.Group("/api"), &Deps{})
	request := httptest.NewRequest(http.MethodPost, "/api/updates/start", strings.NewReader(`{"action":"upgrade","version":"v1.0.0"}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("unauthenticated installation status=%d", response.Code)
	}
	response = httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/updates/status", nil))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"available":false`) {
		t.Fatalf("status=%d %s", response.Code, response.Body.String())
	}
}
