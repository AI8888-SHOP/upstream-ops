package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

// A match beyond the normal first page must be returned, counted once even
// when several groups match, and searched literally rather than as SQL LIKE.
func TestChannelsSearchGroupsBeforePagination(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	channels, rates := storage.NewChannels(db), storage.NewRates(db)
	fixtures := []struct {
		name   string
		groups []storage.RateSnapshot
	}{
		{"first", []storage.RateSnapshot{{ModelName: "default"}}},
		{"second", []storage.RateSnapshot{{ModelName: "Claude-Pro"}, {ModelName: "Claude-Lite"}}},
		{"third", []storage.RateSnapshot{{ModelName: "premium", Description: "Claude 专属分组"}}},
		{"literal", []storage.RateSnapshot{{ModelName: "100%_VIP!"}}},
	}
	for _, fixture := range fixtures {
		item := &storage.Channel{Name: fixture.name, Type: storage.ChannelTypeNewAPI, SiteURL: "https://example.invalid", Username: "test", PasswordCipher: "fixture"}
		if err := channels.Create(item); err != nil {
			t.Fatal(err)
		}
		for _, group := range fixture.groups {
			group.ChannelID = item.ID
			if _, err := rates.Upsert(&group); err != nil {
				t.Fatal(err)
			}
		}
	}
	router := gin.New()
	router.GET("/channels", func(c *gin.Context) { listChannels(c, &Deps{Channels: channels}) })
	for _, tc := range []struct {
		name, term, page, size string
		wantTotal, wantPages   int
		wantNames              []string
	}{
		{"all", "", "1", "-1", 4, 1, []string{"first", "second", "third", "literal"}},
		{"case insensitive and beyond page one", " cLaUdE ", "1", "1", 2, 2, []string{"second"}},
		{"description on second result page", "Claude", "2", "1", 2, 2, []string{"third"}},
		{"several matching groups do not duplicate a channel", "Claude", "1", "-1", 2, 1, []string{"second", "third"}},
		{"percent is literal", "%", "1", "9", 1, 1, []string{"literal"}},
		{"underscore is literal", "_", "1", "9", 1, 1, []string{"literal"}},
		{"escape character is literal", "!", "1", "9", 1, 1, []string{"literal"}},
		{"quote is literal", "' OR 1=1 --", "1", "9", 0, 1, nil},
		{"no match", "missing", "1", "9", 0, 1, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query := url.Values{"search": {tc.term}, "page": {tc.page}, "page_size": {tc.size}}
			response := httptest.NewRecorder()
			router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/channels?"+query.Encode(), nil))
			if response.Code != http.StatusOK {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			var payload struct {
				Data struct {
					Items []struct {
						Name string `json:"name"`
					} `json:"items"`
					Total  int    `json:"total"`
					Pages  int    `json:"pages"`
					Search string `json:"search"`
				} `json:"data"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
				t.Fatal(err)
			}
			if payload.Data.Total != tc.wantTotal || payload.Data.Pages != tc.wantPages || len(payload.Data.Items) != len(tc.wantNames) {
				t.Fatalf("unexpected result: %+v", payload.Data)
			}
			for i, name := range tc.wantNames {
				if payload.Data.Items[i].Name != name {
					t.Fatalf("item[%d]=%q want %q", i, payload.Data.Items[i].Name, name)
				}
			}
		})
	}
}
