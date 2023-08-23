package config

import (
	"fmt"
	"github.com/ahmetson/os-lib/net"
	zmq "github.com/pebbe/zmq4"
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

// SocketType gets the ZMQ analog of the handler type
func SocketType(handlerType HandlerType) zmq.Type {
	if handlerType == SyncReplierType {
		return zmq.REP
	} else if handlerType == ReplierType {
		return zmq.ROUTER
	} else if handlerType == PusherType {
		return zmq.PUSH
	} else if handlerType == PublisherType {
		return zmq.PUB
	}

	return zmq.Type(-1)
}

// ClientSocketType gets the ZMQ analog of the handler type for the clients
func ClientSocketType(handlerType HandlerType) zmq.Type {
	if handlerType == SyncReplierType {
		return zmq.REQ
	} else if handlerType == ReplierType {
		return zmq.DEALER
	} else if handlerType == PusherType {
		return zmq.PULL
	} else if handlerType == PublisherType {
		return zmq.SUB
	}

	return zmq.Type(-1)
}

// HandleUrl creates url of the server url for binding.
// For clients to connect to this url, call client.ClientUrl()
func HandleUrl(id string, port uint64) string {
	if port == 0 {
		return fmt.Sprintf("inproc://%s", id)
	}
	url := fmt.Sprintf("tcp://*:%d", port)
	return url
}
