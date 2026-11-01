package api

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

// parseGatewayAPICallQuery accepts Unix seconds for since/until so callers do
// not depend on database timestamp formatting or server timezone.
func parseGatewayAPICallQuery(c *gin.Context) storage.GatewayAPICallLogQuery {
	q := storage.GatewayAPICallLogQuery{
		Purpose:          strings.TrimSpace(c.Query("purpose")),
		SourceAPIKeyName: strings.TrimSpace(c.Query("source_api_key_name")),
		Model:            strings.TrimSpace(c.Query("model")),
		Status:           strings.TrimSpace(c.Query("status")),
		Page:             queryInt(c, "page", 1),
		PageSize:         queryInt(c, "page_size", 50),
	}
	if value := strings.TrimSpace(c.Query("channel_id")); value != "" {
		if n, err := strconv.ParseUint(value, 10, 64); err == nil {
			q.ChannelID = uint(n)
		}
	}
	if value := strings.TrimSpace(c.Query("provider_id")); value != "" {
		if n, err := strconv.ParseUint(value, 10, 64); err == nil {
			q.GatewayProviderID = uint(n)
		}
	}
	if value := strings.TrimSpace(c.Query("source_group_id")); value != "" {
		if n, err := strconv.ParseInt(value, 10, 64); err == nil {
			q.SourceGroupID = &n
		}
	}
	if value := strings.TrimSpace(c.Query("since")); value != "" {
		if n, err := strconv.ParseInt(value, 10, 64); err == nil {
			t := time.Unix(n, 0).UTC()
			q.Since = &t
		}
	}
	if value := strings.TrimSpace(c.Query("until")); value != "" {
		if n, err := strconv.ParseInt(value, 10, 64); err == nil {
			t := time.Unix(n, 0).UTC()
			q.Until = &t
		}
	}
	return q
}

func listGatewayAPICalls(c *gin.Context, d *Deps) {
	if d == nil || d.GatewayUsage == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "api call audit storage unavailable"})
		return
	}
	page, err := d.GatewayUsage.APICalls().List(parseGatewayAPICallQuery(c))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, page)
}

func statsGatewayAPICalls(c *gin.Context, d *Deps) {
	if d == nil || d.GatewayUsage == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "api call audit storage unavailable"})
		return
	}
	stats, err := d.GatewayUsage.APICalls().Stats(parseGatewayAPICallQuery(c))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, stats)
}
