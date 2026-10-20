package api

import (
	"context"
	"encoding/json"
	"math"
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

func TestDailyReportGatewayUsageProfitHTTPNullAndZero(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rate := func(value float64) *float64 { return &value }
	for _, tc := range []struct {
		name            string
		sellRate        *float64
		missingGroup    bool
		zeroUsage       bool
		revenue, profit *float64
		margin          *float64
		unpriced        int64
	}{
		{name: "configured rate", sellRate: rate(.5), revenue: rate(.5), profit: rate(.25), margin: rate(50)},
		{name: "free rate emits numeric zero", sellRate: rate(0), revenue: rate(0), profit: rate(-.25)},
		{name: "break even emits zero profit and margin", sellRate: rate(.25), revenue: rate(.25), profit: rate(0), margin: rate(0)},
		{name: "unset rate emits null", unpriced: 1},
		{name: "missing group emits null", missingGroup: true, unpriced: 1},
		{name: "zero usage emits numeric zero", sellRate: rate(.5), zeroUsage: true, revenue: rate(0), profit: rate(0)},
		{name: "zero usage without rate emits zero", zeroUsage: true, revenue: rate(0), profit: rate(0)},
		{name: "zero usage without group emits zero", missingGroup: true, zeroUsage: true, revenue: rate(0), profit: rate(0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := openTestDB(t)
			usage := storage.NewGatewayUsageLogs(db)
			groupID := uint(99999)
			if !tc.missingGroup {
				group := &storage.GatewayGroup{Name: "profit-http", SellRateMultiplier: tc.sellRate}
				if err := storage.NewGatewayGroups(db).Create(group); err != nil {
					t.Fatal(err)
				}
				groupID = group.ID
			}
			// The upstream settlement rate is 0.5, so billed cost 0.5 means
			// a standard sales base of 1, not the reference upstream cost 0.25.
			row := storage.GatewayUsageLog{
				RequestID: "profit-http", GatewayGroupID: groupID, ChannelID: 1,
				SourceGroupName: "source", RequestedModel: "model", BillingMode: "token",
				ActualCost: .25, BilledCost: .5, BillingRateMultiplier: .5,
				InputTokens: 10,
				Winner:      true, Success: true, CreatedAt: time.Date(2026, 10, 20, 17, 0, 0, 0, time.UTC),
			}
			if tc.zeroUsage {
				row.InputTokens, row.ActualCost, row.BilledCost = 0, 0, 0
			}
			if err := usage.Create(&row); err != nil {
				t.Fatal(err)
			}
			router := gin.New()
			router.GET("/report", func(c *gin.Context) { dailyReportGatewayUsage(c, &Deps{GatewayUsage: usage}) })
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/report?date=2026-10-21", nil))
			if recorder.Code != http.StatusOK {
				t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
			}
			var payload struct {
				Today map[string]json.RawMessage `json:"today"`
			}
			if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
				t.Fatal(err)
			}
			assertMetrics := func(where string, object map[string]json.RawMessage) {
				t.Helper()
				for key, want := range map[string]*float64{
					"revenue": tc.revenue, "profit": tc.profit, "profit_margin": tc.margin,
				} {
					raw, present := object[key]
					if !present {
						t.Fatalf("%s omitted %s", where, key)
					}
					if want == nil {
						if string(raw) != "null" {
							t.Fatalf("%s.%s = %s, want null", where, key, raw)
						}
						continue
					}
					var got float64
					if string(raw) == "null" || json.Unmarshal(raw, &got) != nil || math.Abs(got-*want) > 1e-9 {
						t.Fatalf("%s.%s = %s, want numeric %v", where, key, raw, *want)
					}
				}
				var unpriced int64
				raw, present := object["unpriced_requests"]
				if !present || string(raw) == "null" || json.Unmarshal(raw, &unpriced) != nil || unpriced != tc.unpriced {
					t.Fatalf("%s.unpriced_requests = %s, want %d", where, raw, tc.unpriced)
				}
				var zeroUsage, wantZeroUsage int64
				if tc.zeroUsage {
					wantZeroUsage = 1
				}
				raw, present = object["zero_usage_requests"]
				if !present || string(raw) == "null" || json.Unmarshal(raw, &zeroUsage) != nil || zeroUsage != wantZeroUsage {
					t.Fatalf("%s.zero_usage_requests = %s, want %d", where, raw, wantZeroUsage)
				}
			}
			assertMetrics("today", payload.Today)
			object := payload.Today
			for _, key := range []string{"sources", "groups", "models"} {
				var children []map[string]json.RawMessage
				if err := json.Unmarshal(object[key], &children); err != nil || len(children) != 1 {
					t.Fatalf("%s = %s, err=%v", key, object[key], err)
				}
				object = children[0]
				assertMetrics(key, object)
			}
		})
	}
}
