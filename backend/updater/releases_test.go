package updater

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCatalogUsesStableVersionsAndExactlyThreePreviousReleases(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`[{"tag_name":"v1.0.3"},{"tag_name":"v1.0.1"},{"tag_name":"v1.0.5"},{"tag_name":"v1.0.4"},{"tag_name":"v1.0.2"},{"tag_name":"v9.0.0","draft":true},{"tag_name":"v8.0.0","prerelease":true},{"tag_name":"v2.0.0-rc1"},{"tag_name":"v01.0.0"}]`))
	}))
	defer server.Close()
	source := releaseSource{client: server.Client(), url: server.URL}
	items, err := source.list(context.Background(), true)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := catalogFor("1.0.4", items)
	if err != nil {
		t.Fatal(err)
	}
	if catalog.Latest.Tag != "v1.0.5" || !catalog.UpdateAvailable || len(catalog.Rollback) != 3 {
		t.Fatalf("catalog=%+v", catalog)
	}
	for i, tag := range []string{"v1.0.3", "v1.0.2", "v1.0.1"} {
		if catalog.Rollback[i].Tag != tag {
			t.Fatalf("rollback=%+v", catalog.Rollback)
		}
	}
	for _, request := range []StartRequest{
		{Current: "1.0.4", Version: "v1.0.5", Action: "upgrade"},
		{Current: "1.0.4", Version: "v1.0.2", Action: "rollback"},
	} {
		if _, err := selectRelease(request.Current, request.Version, request.Action, items); err != nil {
			t.Fatal(err)
		}
	}
	for _, request := range []StartRequest{
		{Current: "dev", Version: "v1.0.5", Action: "upgrade"},
		{Current: "1.0.5", Version: "v1.0.1", Action: "rollback"},
		{Current: "1.0.4", Version: "v1.0.5", Action: "rollback"},
		{Current: "1.0.4", Version: "v1.0.3", Action: "upgrade"},
		{Current: "1.0.4", Version: "v1.0.5; touch /tmp/file", Action: "upgrade"},
	} {
		if _, err := selectRelease(request.Current, request.Version, request.Action, items); err == nil {
			t.Fatalf("accepted %+v", request)
		}
	}
}

func TestReleaseAssetMustBelongToConfiguredRepository(t *testing.T) {
	release := Release{Tag: "v1.0.0", Assets: []Asset{{Name: "archive", URL: "https://evil.invalid/archive"}}}
	if _, err := assetFor(release, "archive"); err == nil {
		t.Fatal("accepted arbitrary artifact URL")
	}
}
