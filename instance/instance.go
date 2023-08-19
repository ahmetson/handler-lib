package instance

import (
	"fmt"
	"github.com/ahmetson/client-lib"
	"github.com/ahmetson/common-lib/data_type/key_value"
	"github.com/ahmetson/common-lib/message"
	"github.com/ahmetson/handler-lib/config"
	"github.com/ahmetson/os-lib/net"
	"github.com/ahmetson/os-lib/process"
	zmq "github.com/pebbe/zmq4"
)

// The Instance is the socket wrapper for the handler instance
//
// The instances have three sockets:
// Push to send its status to the handler
// Sub to receive the messages from the handler
// HandlerType socket to handle the messages.
//
// If parent is PUB
// Then, Instances are per Topic. The instances are sub that receives the messages and then sends it
// to the parent.
//
// If parent is Replier
// Then, the user manages Instances.
//
// The publisher returns two clients.
type Instance struct {
	Id                 string
	parentId           string
	socket             *zmq.Socket
	controllerType     config.HandlerType
	routes             *key_value.KeyValue
	routeDeps          *key_value.KeyValue
	depClients         *client.Clients
	requiredExtensions []string
	extensionConfigs   key_value.KeyValue
	logger             *log.Logger
}

// New handler of the handlerType
func New(handlerType config.HandlerType, id string, parentId string, logger *log.Logger) *Instance {
	return &Instance{
		Id:             id,
		parentId:       parentId,
		controllerType: handlerType,
		routes:         nil,
		routeDeps:      nil,
		depClients:     nil,
		logger:         logger,
	}
}

// A reply sends to the caller the message.
//
// If a server doesn't support replying (for example, PULL server),
// then it returns success.
func (c *Instance) reply(socket *zmq.Socket, message message.Reply) error {
	reply, _ := message.String()
	if _, err := socket.SendMessage(reply); err != nil {
		return fmt.Errorf("recv error replying error %w" + err.Error())
	}

	return nil
}

// Calls server.reply() with the error message.
func (c *Instance) replyError(socket *zmq.Socket, err error) error {
	request := message.Request{}
	return c.reply(socket, request.Fail(err.Error()))
}

// SetRoutes set the reference to the functions and dependencies from the Handler.
func (c *Instance) SetRoutes(routes *key_value.KeyValue, routeDeps *key_value.KeyValue) {
	c.routes = routes
	c.routeDeps = routeDeps
}

// SetClients set the reference to the socket clients
func (c *Instance) SetClients(clients *client.Clients) {
	c.depClients = clients
}

// Type returns the type of the instances
func (c *Instance) Type() config.HandlerType {
	return c.controllerType
}

func (c *Instance) Close() error {
	if c.socket == nil {
		return nil
	}

	err := c.socket.Close()
	if err != nil {
		return fmt.Errorf("server.socket.Close: %w", err)
	}

	return nil
}

func url(name string, port uint64) string {
	if port == 0 {
		return fmt.Sprintf("inproc://%s", name)
	}
	url := fmt.Sprintf("tcp://*:%d", port)
	return url
}

func getSocket(handlerType config.HandlerType) zmq.Type {
	if handlerType == config.SyncReplierType {
		return zmq.REP
	} else if handlerType == config.ReplierType {
		return zmq.ROUTER
	} else if handlerType == config.PusherType {
		return zmq.PUSH
	} else if handlerType == config.PublisherType {
		return zmq.PUB
	}

	return zmq.Type(-1)
}

func bind(sock *zmq.Socket, url string, port uint64) error {
	if err := sock.Bind(url); err != nil {
		if port > 0 {
			// for now, the host name is hardcoded. later we need to get it from the orchestra
			if net.IsPortUsed("localhost", port) {
				pid, err := process.PortToPid(port)
				if err != nil {
					err = fmt.Errorf("config.PortToPid(%d): %w", port, err)
				} else {
					currentPid := process.CurrentPid()
					if currentPid == pid {
						err = fmt.Errorf("another dependency is using it within this orchestra")
					} else {
						err = fmt.Errorf("operating system uses it for another service. pid=%d", pid)
					}
				}
			} else {
				err = fmt.Errorf(`server.socket.bind("tcp://*:%d)": %w`, port, err)
			}
			return err
		} else {
			return fmt.Errorf(`server.socket.bind("inproc://%s"): %w`, url, err)
		}
	}

	return nil
}
