package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

func TestParseGatewayDailyReportDateUsesShanghaiAndRejectsInvalidInput(t *testing.T) {
	now := time.Date(2026, 10, 20, 17, 0, 0, 0, time.UTC)
	date, err := parseGatewayDailyReportDate("", "", now)
	if err != nil || date.Format("2006-01-02") != "2026-10-21" || date.Location().String() != "Asia/Shanghai" || date.UTC().Hour() != 16 {
		t.Fatalf("default date=%v err=%v", date, err)
	}
	date, err = parseGatewayDailyReportDate("2026-10-21", "UTC", now)
	if err != nil || date.Hour() != 0 || date.Location() != time.UTC {
		t.Fatalf("explicit UTC=%v err=%v", date, err)
	}
	for _, input := range [][2]string{{"2026-02-30", "UTC"}, {"2026-10-21T00:00:00Z", "UTC"}, {"2026-10-21", "bad-timezone"}} {
		if _, err := parseGatewayDailyReportDate(input[0], input[1], now); err == nil {
			t.Fatalf("accepted invalid input %v", input)
		}
	}
	// Local calendar arithmetic must not assume every day has 24 hours.
	date, err = parseGatewayDailyReportDate("2026-03-30", "Europe/Vienna", now)
	if err != nil || date.Sub(date.AddDate(0, 0, -1)) != 23*time.Hour {
		t.Fatalf("DST boundary=%v err=%v", date, err)
	}
}

func TestDailyReportGatewayUsageHTTPResponseAndValidation(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	usage := storage.NewGatewayUsageLogs(db)
	row := storage.GatewayUsageLog{RequestID: "daily-api", ChannelID: 1, SourceGroupName: "source", RequestedModel: "model", ActualCost: .25, Winner: true, Success: true, CreatedAt: time.Date(2026, 10, 20, 17, 0, 0, 0, time.UTC)}
	if err := usage.Create(&row); err != nil {
		t.Fatal(err)
	}
	router := gin.New()
	router.GET("/report", func(c *gin.Context) { dailyReportGatewayUsage(c, &Deps{GatewayUsage: usage}) })
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/report?date=2026-10-21", nil))
	if recorder.Code != http.StatusOK || recorder.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var report storage.GatewayDailyReport
	if err := json.Unmarshal(recorder.Body.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report.Today.Requests != 1 || report.Today.Cost != .25 || report.Timezone != "Asia/Shanghai" {
		t.Fatalf("report=%+v", report)
	}
	for _, path := range []string{"/report?date=invalid", "/report?timezone=invalid"} {
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("invalid request %q status=%d", path, recorder.Code)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := usage.DailyReport(ctx, time.Now()); err == nil {
		t.Fatal("canceled request ran report")
	}
}
