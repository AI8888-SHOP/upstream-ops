package api

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/bejix/upstream-ops/backend/gateway"
	"github.com/gin-gonic/gin"
)

// gatewayOptimizerPlan 产出待确认的一键优化方案；只读，不改配置。
func gatewayOptimizerPlan(c *gin.Context, d *Deps) {
	window := 0
	if raw := strings.TrimSpace(c.Query("window_hours")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil {
			window = parsed
		}
	}
	plan, err := d.Gateway.BuildOptimizerPlan(c.Request.Context(), window)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, plan)
}

// gatewayOptimizerApply 应用用户勾选的调整项。
func gatewayOptimizerApply(c *gin.Context, d *Deps) {
	var in struct {
		ChangeIDs   []string `json:"change_ids"`
		WindowHours int      `json:"window_hours"`
	}
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}
	if len(in.ChangeIDs) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "没有选中任何调整项"})
		return
	}
	applied, err := d.Gateway.ApplyOptimizerPlan(gateway.OptimizerApplyInput{
		ChangeIDs: in.ChangeIDs, WindowHours: in.WindowHours,
	})
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error(), "applied": applied})
		return
	}
	c.JSON(http.StatusOK, gin.H{"applied": applied})
}

// gatewayOptimizerExplain 用规则文案或可选的模型接口把方案翻成人话。
func gatewayOptimizerExplain(c *gin.Context, d *Deps) {
	var in struct {
		WindowHours int      `json:"window_hours"`
		ChangeIDs   []string `json:"change_ids"`
	}
	_ = c.ShouldBindJSON(&in)
	plan, err := d.Gateway.BuildOptimizerPlan(c.Request.Context(), in.WindowHours)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	text, source := d.Gateway.ExplainOptimizerPlan(c.Request.Context(), plan, in.ChangeIDs)
	c.JSON(http.StatusOK, gin.H{"explanation": text, "source": source})
}
