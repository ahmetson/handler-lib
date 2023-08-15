package internal

import (
	"github.com/ahmetson/handler-lib/config"
)

func ManagerName(fileName string) string {
	return "manager." + fileName
}

func ContextName(fileName string) string {
	return "orchestra." + fileName
}

func InternalConfiguration(name string) *config.Handler {
	instance := config.Instance{
		Port:               0, // 0 means it's inproc
		Id:                 name + "_instance",
		ControllerCategory: name,
	}

	return &config.Handler{
		Type:      config.SyncReplierType,
		Category:  name,
		Instances: []config.Instance{instance},
	}
}

// ClientUrlParameters return the endpoint to connect to this handler from other services
func ClientUrlParameters(name string) (string, uint64) {
	return name, 0
}
