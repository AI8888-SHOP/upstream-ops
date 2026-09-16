package api

import (
	"errors"
	"net/http"
	"net/url"
	"os"
	"path/filepath"

	"github.com/bejix/upstream-ops/backend/global"
	"github.com/bejix/upstream-ops/backend/updater"
	"github.com/gin-gonic/gin"
)

func updaterSocket(d *Deps) string {
	if socket := os.Getenv("UPDATER_SOCKET"); socket != "" {
		return socket
	}
	if d != nil && d.Runtime != nil {
		return filepath.Join(filepath.Dir(d.Runtime.ConfigPath()), ".updater", "control.sock")
	}
	return ""
}

func registerUpdates(api *gin.RouterGroup, d *Deps) {
	// Installing executable code must never inherit the optional no-auth mode
	// of ordinary read-only admin pages. The normal middleware authenticates
	// enabled sessions; this additional guard requires auth to remain enabled.
	guard := func(c *gin.Context) bool {
		if d == nil || d.Runtime == nil || d.Runtime.CurrentAuth() == nil {
			fail(c, http.StatusForbidden, errors.New("一键升级和回退需要先开启后台登录鉴权"))
			return false
		}
		return true
	}
	api.GET("/updates/status", func(c *gin.Context) {
		if d == nil || d.Runtime == nil || d.Runtime.CurrentAuth() == nil {
			c.JSON(http.StatusOK, updater.Status{Reason: "开启后台登录鉴权后，可启用一键升级与回退"})
			return
		}
		var status updater.Status
		if err := updater.Call(c.Request.Context(), updaterSocket(d), http.MethodGet, "/status", nil, &status); err != nil {
			c.JSON(http.StatusOK, updater.Status{Reason: "未连接独立更新进程。Docker 请启用 updater 服务；原生部署请启用 updater systemd 服务。"})
			return
		}
		c.JSON(http.StatusOK, status)
	})
	api.GET("/updates/releases", func(c *gin.Context) {
		if !guard(c) {
			return
		}
		var catalog updater.Catalog
		path := "/releases?current=" + url.QueryEscape(global.VERSION)
		if c.Query("force") == "1" {
			path += "&force=1"
		}
		if err := updater.Call(c.Request.Context(), updaterSocket(d), http.MethodGet, path, nil, &catalog); err != nil {
			fail(c, http.StatusBadGateway, err)
			return
		}
		c.JSON(http.StatusOK, catalog)
	})
	api.POST("/updates/start", func(c *gin.Context) {
		if !guard(c) {
			return
		}
		var input struct {
			Version string `json:"version"`
			Action  string `json:"action"`
		}
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 4096)
		if err := c.ShouldBindJSON(&input); err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		request := updater.StartRequest{Current: global.VERSION, Version: input.Version, Action: input.Action}
		var status updater.Status
		if err := updater.Call(c.Request.Context(), updaterSocket(d), http.MethodPost, "/start", request, &status); err != nil {
			fail(c, http.StatusConflict, err)
			return
		}
		c.JSON(http.StatusAccepted, status)
	})
}
