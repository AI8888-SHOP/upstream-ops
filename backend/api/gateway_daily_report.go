package api

import (
	"fmt"
	"net/http"
	"strings"
	"time"
	_ "time/tzdata" // Reports use explicit IANA zones on every release platform.

	"github.com/gin-gonic/gin"
)

func parseGatewayDailyReportDate(rawDate, rawTimezone string, now time.Time) (time.Time, error) {
	zone := strings.TrimSpace(rawTimezone)
	if zone == "" {
		zone = "Asia/Shanghai"
	}
	if len(zone) > 100 {
		return time.Time{}, fmt.Errorf("invalid reporting timezone")
	}
	loc, err := time.LoadLocation(zone)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid reporting timezone")
	}
	value := strings.TrimSpace(rawDate)
	if value == "" {
		value = now.In(loc).Format("2006-01-02")
	}
	date, err := time.ParseInLocation("2006-01-02", value, loc)
	if err != nil {
		return time.Time{}, fmt.Errorf("date must be YYYY-MM-DD")
	}
	return date, nil
}

func dailyReportGatewayUsage(c *gin.Context, d *Deps) {
	date, err := parseGatewayDailyReportDate(c.Query("date"), c.Query("timezone"), time.Now())
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if d == nil || d.GatewayUsage == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "usage storage unavailable"})
		return
	}
	report, err := d.GatewayUsage.DailyReport(c.Request.Context(), date)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.Header("Cache-Control", "no-store")
	c.JSON(http.StatusOK, report)
}
