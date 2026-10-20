package api

import (
	"github.com/gin-gonic/gin"
	"net/http"
)

func syncGatewayPrices(c *gin.Context, d *Deps) {
	if d.Gateway == nil || d.ModelPrices == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "gateway unavailable"})
		return
	}
	result, err := d.Gateway.SyncModelPrices(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, result)
}

func gatewayPriceRepricingStatus(c *gin.Context, d *Deps) {
	if d.ModelPrices == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "price storage unavailable"})
		return
	}
	job, err := d.ModelPrices.RepricingStatus(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, job)
}

func restartGatewayPriceRepricing(c *gin.Context, d *Deps) {
	if d.ModelPrices == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "price storage unavailable"})
		return
	}
	_, job, err := d.ModelPrices.ApplyWithRepricing(c.Request.Context(), nil, 0, false)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if d.Gateway != nil {
		d.Gateway.WakePriceRepricing()
	}
	c.JSON(http.StatusAccepted, job)
}
