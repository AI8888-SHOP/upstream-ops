package gateway

import (
	"net/http"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/gin-gonic/gin"
)

// endpointsPayload 对齐 CLIProxyAPI / sub2api 的服务说明形态。
// 注意：不要占用 GET /gateway —— 那是前端 SPA 管理页路由。
func endpointsPayload() gin.H {
	return gin.H{
		"message": "UpstreamOps Gateway",
		"endpoints": []string{
			// OpenAI Chat
			"POST /v1/chat/completions",
			"POST /chat/completions",
			"POST /v1/completions",
			// OpenAI Responses
			"POST /v1/responses",
			"POST /v1/responses/*",
			"GET  /v1/responses",
			"POST /responses",
			"POST /responses/*",
			"GET  /responses",
			// Anthropic
			"POST /v1/messages",
			"POST /v1/messages/count_tokens",
			// Models / Usage
			"GET  /v1/models",
			"GET  /v1/usage",
			// Embeddings / Images / Videos / Alpha
			"POST /v1/embeddings",
			"POST /embeddings",
			"POST /v1/images/generations",
			"POST /v1/images/edits",
			"POST /images/generations",
			"POST /images/edits",
			"POST /v1/videos/generations",
			"POST /v1/videos",
			"POST /v1/videos/edits",
			"POST /v1/videos/extensions",
			"GET  /v1/videos/:request_id",
			"GET  /v1/videos/:request_id/content",
			"POST /v1/alpha/search",
			// Codex
			"POST /backend-api/codex/responses",
			"POST /backend-api/codex/responses/*",
			"GET  /backend-api/codex/responses",
			"GET  /backend-api/codex/models",
			// Gemini
			"GET  /v1beta/models",
			"POST /v1beta/models/*",
			// Antigravity
			"POST /antigravity/v1/messages",
			"GET  /antigravity/v1/models",
		},
	}
}

// RegisterPublic 注册网关公开路由（不走管理端鉴权）。
func RegisterPublic(r *gin.Engine, s *Service) {
	r.GET("/", func(c *gin.Context) {
		c.JSON(http.StatusOK, endpointsPayload())
	})
	r.GET("/v1", func(c *gin.Context) {
		c.JSON(http.StatusOK, endpointsPayload())
	})
	registerAllGatewayRoutes(r, s)
}

// RegisterPublicWithoutRoot 注册网关路由但不占用 GET /（有 SPA 时使用）。
func RegisterPublicWithoutRoot(r *gin.Engine, s *Service) {
	r.GET("/v1", func(c *gin.Context) {
		c.JSON(http.StatusOK, endpointsPayload())
	})
	registerAllGatewayRoutes(r, s)
}

func registerAllGatewayRoutes(r *gin.Engine, s *Service) {
	// ---------- /v1 ----------
	v1 := r.Group("/v1")
	{
		// Models / Usage
		v1.GET("/models", s.HandleModels)
		v1.GET("/usage", s.HandleUsage)

		// OpenAI Chat Completions
		v1.POST("/chat/completions", func(c *gin.Context) {
			s.HandleForward(c, "/v1/chat/completions", protocol.KindOpenAIChat)
		})
		v1.POST("/completions", func(c *gin.Context) {
			s.HandleForward(c, "/v1/completions", protocol.KindOpenAIChat)
		})

		// OpenAI Responses
		v1.POST("/responses", func(c *gin.Context) {
			s.HandleForward(c, "/v1/responses", protocol.KindOpenAIResponses)
		})
		v1.POST("/responses/*subpath", func(c *gin.Context) {
			sub := c.Param("subpath")
			s.HandleForward(c, "/v1/responses"+sub, protocol.KindOpenAIResponses)
		})
		v1.GET("/responses", s.HandleResponsesWebSocket)

		// Anthropic Messages
		v1.POST("/messages", func(c *gin.Context) {
			s.HandleForward(c, "/v1/messages", protocol.KindAnthropic)
		})
		v1.POST("/messages/count_tokens", s.HandleCountTokens)

		// Embeddings
		v1.POST("/embeddings", func(c *gin.Context) {
			s.HandleForward(c, "/v1/embeddings", protocol.KindOpenAIChat)
		})

		// Images
		v1.POST("/images/generations", func(c *gin.Context) {
			s.HandleForward(c, "/v1/images/generations", protocol.KindOpenAIChat)
		})
		v1.POST("/images/edits", func(c *gin.Context) {
			s.HandleForward(c, "/v1/images/edits", protocol.KindOpenAIChat)
		})
		// 批量图像：简化为透传（上游若支持则工作）
		v1.POST("/images/batches", func(c *gin.Context) {
			s.HandleForward(c, "/v1/images/batches", protocol.KindOpenAIChat)
		})
		v1.GET("/images/batches", func(c *gin.Context) {
			s.HandleForward(c, "/v1/images/batches", protocol.KindOpenAIChat)
		})
		v1.GET("/images/batches/models", func(c *gin.Context) {
			s.HandleForward(c, "/v1/images/batches/models", protocol.KindOpenAIChat)
		})
		v1.GET("/images/batches/:id", func(c *gin.Context) {
			s.HandleForward(c, "/v1/images/batches/"+c.Param("id"), protocol.KindOpenAIChat)
		})
		v1.GET("/images/batches/:id/items", func(c *gin.Context) {
			s.HandleForward(c, "/v1/images/batches/"+c.Param("id")+"/items", protocol.KindOpenAIChat)
		})
		v1.GET("/images/batches/:id/items/:custom_id/content", func(c *gin.Context) {
			s.HandleForward(c, "/v1/images/batches/"+c.Param("id")+"/items/"+c.Param("custom_id")+"/content", protocol.KindOpenAIChat)
		})
		v1.GET("/images/batches/:id/download", func(c *gin.Context) {
			s.HandleForward(c, "/v1/images/batches/"+c.Param("id")+"/download", protocol.KindOpenAIChat)
		})
		v1.POST("/images/batches/:id/cancel", func(c *gin.Context) {
			s.HandleForward(c, "/v1/images/batches/"+c.Param("id")+"/cancel", protocol.KindOpenAIChat)
		})
		v1.DELETE("/images/batches/:id", func(c *gin.Context) {
			s.HandleForward(c, "/v1/images/batches/"+c.Param("id"), protocol.KindOpenAIChat)
		})
		v1.DELETE("/images/batches/:id/outputs", func(c *gin.Context) {
			s.HandleForward(c, "/v1/images/batches/"+c.Param("id")+"/outputs", protocol.KindOpenAIChat)
		})

		// Videos
		v1.POST("/videos", func(c *gin.Context) {
			s.HandleForward(c, "/v1/videos", protocol.KindOpenAIChat)
		})
		v1.POST("/videos/generations", func(c *gin.Context) {
			s.HandleForward(c, "/v1/videos/generations", protocol.KindOpenAIChat)
		})
		v1.POST("/videos/edits", func(c *gin.Context) {
			s.HandleForward(c, "/v1/videos/edits", protocol.KindOpenAIChat)
		})
		v1.POST("/videos/extensions", func(c *gin.Context) {
			s.HandleForward(c, "/v1/videos/extensions", protocol.KindOpenAIChat)
		})
		v1.GET("/videos/:request_id", func(c *gin.Context) {
			s.HandleForward(c, "/v1/videos/"+c.Param("request_id"), protocol.KindOpenAIChat)
		})
		v1.GET("/videos/:request_id/content", func(c *gin.Context) {
			s.HandleForward(c, "/v1/videos/"+c.Param("request_id")+"/content", protocol.KindOpenAIChat)
		})

		// Alpha Search
		v1.POST("/alpha/search", func(c *gin.Context) {
			s.HandleForward(c, "/v1/alpha/search", protocol.KindOpenAIChat)
		})
	}

	// ---------- 无 /v1 前缀别名 ----------
	r.POST("/chat/completions", func(c *gin.Context) {
		s.HandleForward(c, "/v1/chat/completions", protocol.KindOpenAIChat)
	})
	r.POST("/embeddings", func(c *gin.Context) {
		s.HandleForward(c, "/v1/embeddings", protocol.KindOpenAIChat)
	})
	r.POST("/responses", func(c *gin.Context) {
		s.HandleForward(c, "/v1/responses", protocol.KindOpenAIResponses)
	})
	r.POST("/responses/*subpath", func(c *gin.Context) {
		s.HandleForward(c, "/v1/responses"+c.Param("subpath"), protocol.KindOpenAIResponses)
	})
	r.GET("/responses", s.HandleResponsesWebSocket)
	r.POST("/alpha/search", func(c *gin.Context) {
		s.HandleForward(c, "/v1/alpha/search", protocol.KindOpenAIChat)
	})
	r.POST("/images/generations", func(c *gin.Context) {
		s.HandleForward(c, "/v1/images/generations", protocol.KindOpenAIChat)
	})
	r.POST("/images/edits", func(c *gin.Context) {
		s.HandleForward(c, "/v1/images/edits", protocol.KindOpenAIChat)
	})
	r.POST("/videos", func(c *gin.Context) {
		s.HandleForward(c, "/v1/videos", protocol.KindOpenAIChat)
	})
	r.POST("/videos/generations", func(c *gin.Context) {
		s.HandleForward(c, "/v1/videos/generations", protocol.KindOpenAIChat)
	})
	r.POST("/videos/edits", func(c *gin.Context) {
		s.HandleForward(c, "/v1/videos/edits", protocol.KindOpenAIChat)
	})
	r.POST("/videos/extensions", func(c *gin.Context) {
		s.HandleForward(c, "/v1/videos/extensions", protocol.KindOpenAIChat)
	})
	r.GET("/videos/:request_id", func(c *gin.Context) {
		s.HandleForward(c, "/v1/videos/"+c.Param("request_id"), protocol.KindOpenAIChat)
	})
	r.GET("/videos/:request_id/content", func(c *gin.Context) {
		s.HandleForward(c, "/v1/videos/"+c.Param("request_id")+"/content", protocol.KindOpenAIChat)
	})

	// ---------- Codex ----------
	codex := r.Group("/backend-api/codex")
	{
		codex.POST("/responses", func(c *gin.Context) {
			s.HandleForward(c, "/v1/responses", protocol.KindOpenAIResponses)
		})
		codex.POST("/responses/*subpath", func(c *gin.Context) {
			s.HandleForward(c, "/v1/responses"+c.Param("subpath"), protocol.KindOpenAIResponses)
		})
		codex.GET("/responses", s.HandleResponsesWebSocket)
		codex.POST("/alpha/search", func(c *gin.Context) {
			s.HandleForward(c, "/v1/alpha/search", protocol.KindOpenAIChat)
		})
		codex.GET("/models", s.HandleModels)
	}

	// ---------- Gemini v1beta ----------
	v1beta := r.Group("/v1beta")
	{
		v1beta.GET("/models", s.HandleGeminiModels)
		v1beta.GET("/models/*model", s.HandleGeminiModels)
		v1beta.POST("/models/*modelAction", s.HandleGeminiGenerate)
	}

	// ---------- Antigravity ----------
	r.GET("/antigravity/models", s.HandleModels)
	ag := r.Group("/antigravity/v1")
	{
		ag.POST("/messages", func(c *gin.Context) {
			s.HandleForward(c, "/v1/messages", protocol.KindAnthropic)
		})
		ag.POST("/messages/count_tokens", s.HandleCountTokens)
		ag.GET("/models", s.HandleModels)
		ag.GET("/usage", s.HandleUsage)
	}
	agBeta := r.Group("/antigravity/v1beta")
	{
		agBeta.GET("/models", s.HandleGeminiModels)
		agBeta.GET("/models/*model", s.HandleGeminiModels)
		agBeta.POST("/models/*modelAction", s.HandleGeminiGenerate)
	}
}
