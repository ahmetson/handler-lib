package config

import (
	"fmt"
	"github.com/ahmetson/os-lib/net"
)

type Handler struct {
	Type           HandlerType
	Category       string
	InstanceAmount uint64
	Port           uint64
	Id             string
}

func NewHandler(as HandlerType, cat string) (*Handler, error) {
	port := net.GetFreePort()
	if port == 0 {
		return nil, fmt.Errorf("net.GetFreePort: no free port")
	}

	control := &Handler{
		Type:           as,
		Category:       cat,
		Id:             cat + "_1",
		InstanceAmount: 1,
		Port:           uint64(port),
	}

	return control, nil
}
