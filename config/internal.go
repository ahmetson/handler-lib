package config

import (
	"fmt"
	"strings"
)

// UrlToFileName converts the given url to the file name. Simply it replaces the slashes with dots.
//
// ExternalUrl returns the full url to connect to the orchestra.
//
// The orchestra url is defined from the main service's url.
//
// For example:
//
//	serviceUrl = "github.com/ahmetson/sample-service"
//	contextUrl = "orchestra.github.com.ahmetson.sample-service"
//
// This controllerName is set as the server's name in the config.
// Then the server package will generate an inproc:// url based on the server name.
func UrlToFileName(url string) string {
	return strings.ReplaceAll(strings.ReplaceAll(url, "/", "."), "\\", ".")
}

func ManagerName(url string) string {
	fileName := UrlToFileName(url)
	return "manager." + fileName
}

func ContextName(url string) string {
	fileName := UrlToFileName(url)
	return "orchestra." + fileName
}

// ParentUrl returns the url of the handler to be connected by the instances
func ParentUrl(handlerId string) string {
	return fmt.Sprintf("inproc://handler_%s", handlerId)
}

// InstanceHandleUrl returns the url of the instance for handling the requests
func InstanceHandleUrl(parentId string, id string) string {
	return fmt.Sprintf("inproc://inst_handle_%s_%s", parentId, id)
}

// InstanceUrl returns the url of the instance for managing the instance itself
func InstanceUrl(parentId string, id string) string {
	return fmt.Sprintf("inproc://inst_manage_%s_%s", parentId, id)
}

// NewInternalHandler returns the configuration with the default parameters
func NewInternalHandler(as HandlerType, cat string) *Handler {
	return &Handler{
		Type:           as,
		Category:       cat,
		Id:             cat + "_1",
		InstanceAmount: 1,
		Port:           0,
	}
}

// ClientUrlParameters return the endpoint to connect to this server from other services
func ClientUrlParameters(name string) (string, uint64) {
	return name, 0
}
