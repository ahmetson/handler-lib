package instance

import (
	"fmt"
	"github.com/ahmetson/client-lib"
	"github.com/ahmetson/common-lib/data_type/key_value"
	"github.com/ahmetson/common-lib/message"
	"github.com/ahmetson/handler-lib/config"
	"github.com/ahmetson/handler-lib/route"
	"github.com/ahmetson/log-lib"
	zmq "github.com/pebbe/zmq4"
)

const (
	CLOSED  = "close"
	READY   = "ready"
	PREPARE = "prepare"
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
	close              bool
	status             string
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
		close:          false,
		status:         PREPARE,
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

// Status of the instance
func (c *Instance) Status() string {
	return c.status
}

func (c *Instance) Run() {
	parent, err := zmq.NewSocket(zmq.PUSH)
	if err != nil {
		c.logger.Warn("failed to create a parent client socket, parent should check it")
		return
	}

	handler, err := zmq.NewSocket(config.GetSocket(c.Type()))
	if err != nil {
		errMsg := fmt.Sprintf("failed to create a handler socket of %s type: %v", c.Type(), err)
		reply := message.Reply{Status: message.FAIL, Parameters: key_value.Empty(), Message: errMsg}
		replyStr, _ := reply.String()
		if _, err := parent.SendMessage(replyStr); err != nil {
			c.logger.Warn("failed to send a message to parent", "message", reply)
			return
		}
	}

	err = handler.Bind(config.InstanceHandleUrl(c.parentId, c.Id))
	if err != nil {
		c.logger.Fatal("bind error", "error", err)
	}

	manage, err := zmq.NewSocket(zmq.REP)
	if err != nil {
		errMsg := fmt.Sprintf("failed to create a manager socket: %v", err)
		reply := message.Reply{Status: message.FAIL, Parameters: key_value.Empty(), Message: errMsg}
		replyStr, _ := reply.String()
		if _, err := parent.SendMessage(replyStr); err != nil {
			c.logger.Warn("failed to send a message to parent", "message", reply)
			return
		}
	}

	err = manage.Bind(config.InstanceUrl(c.parentId, c.Id))
	if err != nil {
		c.logger.Fatal("bind error", "error", err)
	}

	poller := zmq.NewPoller()
	poller.Add(handler, zmq.POLLIN)
	poller.Add(manage, zmq.POLLIN)

	c.status = READY
	c.close = false

	for {
		if c.close {
			c.logger.Warn("received a close signal, stop receiving messages")
			poller.RemoveBySocket(handler)
			poller.RemoveBySocket(manage)
			c.status = CLOSED

			break
		}

		sockets, err := poller.Poll(-1)
		if err != nil {
			newErr := fmt.Errorf("poller.Poll(%s): %w", c.Type(), err)
			c.logger.Fatal("failed", "error", newErr)
		}

		if len(sockets) == 0 {
			continue
		}

		for _, polled := range sockets {
			if polled.Socket == handler {
				data, meta, err := handler.RecvMessageWithMetadata(0)
				if err != nil {
					newErr := fmt.Errorf("socket.recvMessageWithMetadata: %w", err)
					if err := c.replyError(handler, newErr); err != nil {
						c.logger.Fatal("error", "message", err)
					}
					c.logger.Fatal("error", "message", newErr)
				}

				c.logger.Info("message received", "messages", data)

				reply, err := c.processMessage(data, meta)
				if err != nil {
					c.logger.Fatal("processMessage", "message", data, "meta", meta, "error", err)
				}

				if err := c.reply(handler, reply); err != nil {
					c.logger.Fatal("failed")
				}
			} else if polled.Socket == manage {
				// close it
				c.logger.Warn("received a signal, we assume its a close")
				c.close = true
			}
		}
	}

	err = manage.Close()
	if err != nil {
		c.logger.Fatal("manage close", "error", err)
	}

	err = handler.Close()
	if err != nil {
		c.logger.Fatal("handler close", "error", err)
	}

	err = parent.Close()
	if err != nil {
		c.logger.Fatal("parent client close", "error", err)
	}

	c.logger.Warn("end of the instance.Run no more things to do.")
}

func (c *Instance) processMessage(msgRaw []string, metadata map[string]string) (message.Reply, error) {
	// All request types derive from the basic request.
	// We first attempt to parse basic request from the raw message
	request, err := message.NewReqWithMeta(msgRaw, metadata)
	if err != nil {
		newErr := fmt.Errorf("message.ParseRequest: %w", err)

		return message.Reply{}, newErr
	}

	// Add the trace
	//if request.IsFirst() {
	//	request.SetUuid()
	//}
	//request.AddRequestStack(c.serviceUrl, c.config.Category, c.config.Instances[0].Id)

	handleInterface, depNames, err := route.Route(request.Command, *c.routes, *c.routeDeps)
	if err != nil {
		return request.Fail(fmt.Sprintf("route.Route(%s)", request.Command)), nil
	}

	depClients := route.FilterExtensionClients(depNames, *c.depClients)

	reply := route.Handle(request, handleInterface, depClients)

	// update the stack
	//if err = reply.SetStack(c.serviceUrl, c.config.Category, c.config.Instances[0].Id); err != nil {
	//	c.logger.Warn("failed to update the reply stack", "error", err)
	//}

	return *reply, nil
}
