package main

import (
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
)

func main() {
	router := gin.Default()
	router.GET("/ping", func(c *gin.Context) { c.String(http.StatusOK, "pong") })
	router.Run("0.0.0.0:" + os.Getenv("PORT"))
}
