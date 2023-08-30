package config

import (
	"fmt"
	clientConfig "github.com/ahmetson/client-lib/config"
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

// NewHandler configuration of the HandlerType and category.
// It generates the ID of the handler, as well as gets the free port.
//
// If not possible to get the port, it returns an error.
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

// ExternalUrl creates url of the server url for binding.
// For clients to connect to this url, call client.ClientUrl()
func ExternalUrl(id string, port uint64) string {
	if port == 0 {
		return fmt.Sprintf("inproc://%s", id)
	}
	url := fmt.Sprintf("tcp://*:%d", port)
	return url
}

func ExternalUrlByClient(client *clientConfig.Client) string {
	return ExternalUrl(client.Id, client.Port)
}

// CanReply returns true if the given Handler has to reply back to the user.
// It's the opposite of CanTrigger.
func CanReply(handlerType HandlerType) bool {
	return handlerType == ReplierType || handlerType == SyncReplierType
}
