package app

import (
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/koordinator-sh/koord-queue/pkg/controller"
	"net/http"
)

func installAPIHandlers(engine *gin.Engine, controller *controller.Controller) {
	routes := engine.Group("/apis/v1")
	{
		routes.GET("/queue", getQueueDebugInfo(controller))
		routes.GET("/userquota", getUserQuotaDebugInfo(controller))
	}
}

func getQueueDebugInfo(controller *controller.Controller) gin.HandlerFunc {
	return func(c *gin.Context) {
		result := controller.GetQueueDebugInfo()

		resultData, _ := json.MarshalIndent(result, "", "\t")
		c.Data(http.StatusOK, "%s", resultData)
	}
}

func getUserQuotaDebugInfo(controller *controller.Controller) gin.HandlerFunc {
	return func(c *gin.Context) {
		result := controller.GetUserQuotaDebugInfo()

		resultData, _ := json.MarshalIndent(result, "", "\t")
		c.Data(http.StatusOK, "%s", resultData)
	}
}
