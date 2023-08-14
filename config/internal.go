package config

import (
	"strings"
)

// UrlToFileName converts the given url to the file name. Simply it replaces the slashes with dots.
//
// Url returns the full url to connect to the orchestra.
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

func InternalConfiguration(name string) *Handler {
	instance := Instance{
		Port:               0, // 0 means it's inproc
		Id:                 name + "_instance",
		ControllerCategory: name,
	}

	return &Handler{
		Type:      SyncReplierType,
		Category:  name,
		Instances: []Instance{instance},
	}
}

// ClientUrlParameters return the endpoint to connect to this server from other services
func ClientUrlParameters(name string) (string, uint64) {
	return name, 0
}
