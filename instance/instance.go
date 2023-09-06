package instance

import (
	"fmt"
	"github.com/ahmetson/common-lib/data_type/key_value"
	"github.com/ahmetson/common-lib/message"
	"github.com/ahmetson/handler-lib/config"
	"github.com/ahmetson/handler-lib/route"
	"github.com/ahmetson/log-lib"
	zmq "github.com/pebbe/zmq4"
	"time"
)

const (
	PREPARE  = "prepare"  // instance is created, but not yet running
	READY    = "ready"    // instance is running and waiting for messages to handle
	HANDLING = "handling" // instance is running, but busy by handling messages
	CLOSED   = "close"    // instance was closed
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
	Id          string
	parentId    string
	handlerType config.HandlerType
	routes      *key_value.KeyValue // handler routing
	routeDeps   *key_value.KeyValue // handler deps
	depClients  *key_value.KeyValue
	logger      *log.Logger
	close       bool
	status      string // Instance status
	errors      []error
}

// New handler of the handlerType
func New(handlerType config.HandlerType, id string, parentId string, parent *log.Logger) *Instance {
	logger := parent.Child(id)

	return &Instance{
		Id:          id,
		parentId:    parentId,
		handlerType: handlerType,
		routes:      nil,
		routeDeps:   nil,
		depClients:  nil,
		logger:      logger,
		close:       false,
		status:      PREPARE,
		errors:      make([]error, 0),
	}
}

// A reply sends to the caller the message.
//
// If a handler doesn't support replying (for example, PULL handler),
// then it returns success.
func (c *Instance) reply(socket *zmq.Socket, message message.Reply) error {
	if !config.CanReply(c.Type()) {
		return nil
	}

	reply, _ := message.String()
	if len(message.SessionId()) == 0 {
		if _, err := socket.SendMessage(reply); err != nil {
			return fmt.Errorf("recv error replying error %w" + err.Error())
		}
	} else {
		if _, err := socket.SendMessage(message.SessionId(), "", reply); err != nil {
			return fmt.Errorf("recv error replying error %w" + err.Error())
		}
	}

	return nil
}

// Calls handler.reply() with the error message.
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
func (c *Instance) SetClients(clients *key_value.KeyValue) {
	c.depClients = clients
}

// Type returns the type of the instances
func (c *Instance) Type() config.HandlerType {
	return c.handlerType
}

// Status of the instance
func (c *Instance) Status() string {
	return c.status
}

// pubStatus notifies instance manager with the status of this Instance.
func (c *Instance) pubStatus(parent *zmq.Socket, status string) error {
	req := message.Request{
		Command:    "set_status",
		Parameters: key_value.Empty().Set("id", c.Id).Set("status", status),
	}

	reqStr, err := req.String()
	if err != nil {
		return fmt.Errorf("request.String: %w", err)
	}

	_, err = parent.SendMessageDontwait(reqStr)
	if err != nil {
		return fmt.Errorf("parent.SendMessageDontWait: %w", err)
	}

	return nil
}

// pubFail notifies instance manager that this Instance crashed
func (c *Instance) pubFail(parent *zmq.Socket, instanceErr error) error {
	req := message.Request{
		Command:    "set_status",
		Parameters: key_value.Empty().Set("id", c.Id).Set("status", CLOSED).Set("message", instanceErr.Error()),
	}

	reqStr, err := req.String()
	if err != nil {
		return fmt.Errorf("request.String: %w", err)
	}

	_, err = parent.SendMessageDontwait(reqStr)
	if err != nil {
		return fmt.Errorf("parent.SendMessageDontWait: %w", err)
	}

	return nil
}

// Errors that occurred in the instance during data transfer between instance manager and this service.
// So, instance couldn't send it to the instance.
func (c *Instance) Errors() []error {
	return c.errors
}

// Start the instance manager and handler.
func (c *Instance) Start() error {
	ready := make(chan error)

	c.errors = make([]error, 0)

	go func(ready chan error) {
		parent, err := zmq.NewSocket(zmq.PUSH)
		if err != nil {
			ready <- fmt.Errorf("zmq.NewSocket('PUSH'): %w", err)
			return
		}

		parentUrl := config.ParentUrl(c.parentId)
		err = parent.Connect(parentUrl)
		if err != nil {
			ready <- fmt.Errorf("parent.Connect('%s'): %w", parentUrl, err)
			return
		}

		// Notify the parent that it's getting prepared
		err = c.pubStatus(parent, PREPARE)
		if err != nil {
			ready <- fmt.Errorf("c.pubStatus('%s'): %w", PREPARE, err)
			return
		}

		handler, err := zmq.NewSocket(config.SocketType(c.Type()))
		if err != nil {
			ready <- fmt.Errorf("zmq.NewSocket('handler', type: '%s'): %w", c.Type(), err)
			return
		}

		handlerUrl := config.InstanceHandleUrl(c.parentId, c.Id)
		err = handler.Bind(handlerUrl)
		if err != nil {
			ready <- fmt.Errorf("handler.Bind('%s'): %w", handlerUrl, err)
			return
		}

		manager, err := zmq.NewSocket(zmq.REP)
		if err != nil {
			closeErr := handler.Close()
			if closeErr != nil {
				err = fmt.Errorf("%w: handler.Close: %w", err, closeErr)
			}
			ready <- fmt.Errorf("zmq.NewSocket('manager'): %w", err)
			return
		}

		managerUrl := config.InstanceUrl(c.parentId, c.Id)
		err = manager.Bind(managerUrl)
		if err != nil {
			closeErr := handler.Close()
			if closeErr != nil {
				err = fmt.Errorf("%w: handler.Close: %w", err, closeErr)
			}
			ready <- fmt.Errorf("manager.Bind('%s'): %w", managerUrl, err)
			return
		}

		poller := zmq.NewPoller()
		poller.Add(handler, zmq.POLLIN)
		poller.Add(manager, zmq.POLLIN)

		c.status = READY
		c.close = false

		err = c.pubStatus(parent, READY)
		if err != nil {
			closeErr := handler.Close()
			if closeErr != nil {
				err = fmt.Errorf("%w: handler.Close: %w", err, closeErr)
			}
			closeErr = manager.Close()
			if closeErr != nil {
				err = fmt.Errorf("%w: manager.Close: %w", err, closeErr)
			}
			ready <- fmt.Errorf("c.pubStatus('%s'): %w", READY, err)
			return
		}

		// exit from instance.Run
		// the rest of the code are notified by the pusher
		ready <- nil

		for {
			if c.close {
				break
			}

			sockets, err := poller.Poll(time.Millisecond)
			if err != nil {
				newErr := fmt.Errorf("poller.Poll(%s): %w", c.Type(), err)
				pubErr := c.pubFail(parent, newErr)
				if pubErr != nil {
					c.errors = append(c.errors, fmt.Errorf("c.pubFail('%w'): %w", newErr, pubErr))
				}
				break
			}

			if len(sockets) == 0 {
				continue
			}

			for _, polled := range sockets {
				if polled.Socket == handler {
					data, meta, err := handler.RecvMessageWithMetadata(0)
					if err != nil {
						newErr := fmt.Errorf("socket.recvMessageWithMetadata: %w", err)
						err = c.replyError(handler, newErr)
						if err != nil {
							newErr = fmt.Errorf("c.ReplyError('handler', '%w'): %w", newErr, err)
						}
						pubErr := c.pubFail(parent, newErr)
						if pubErr != nil {
							c.errors = append(c.errors, fmt.Errorf("c.pubFail('%w'): %w", newErr, pubErr))
						}
						break
					}

					pubErr := c.pubStatus(parent, HANDLING)
					if pubErr != nil {
						// no need to call for c.pubFail, since that could also lead to the same error as pubStatus.
						// they use the same socket.
						c.errors = append(c.errors, fmt.Errorf("c.pubStatus('HANDLING'): %w", pubErr))
						break
					}

					reply, err := c.processMessage(data, meta)
					if err != nil {
						pubErr := c.pubFail(parent, fmt.Errorf("c.processData: %w", err))
						if pubErr != nil {
							c.errors = append(c.errors, fmt.Errorf("c.pubFail('%w'): %w", err, pubErr))
						}
						break
					}

					pubErr = c.pubStatus(parent, READY)
					if pubErr != nil {
						// no need to call for c.pubFail, since that could also lead to the same error as pubStatus.
						// they use the same socket.
						c.errors = append(c.errors, fmt.Errorf("c.pubStatus('READY'): %w", pubErr))
						break
					}

					if err := c.reply(handler, reply); err != nil {
						failErr := fmt.Errorf("c.reply('handler'): %w", err)
						pubErr := c.pubFail(parent, failErr)
						if pubErr != nil {
							c.errors = append(c.errors, fmt.Errorf("c.pubFail('%w'): %w", failErr, pubErr))
						}
						break
					}
				} else if polled.Socket == manager {
					fmt.Printf("manager received a message")
					// for now, the instance supports only one command: CLOSE
					_, err := manager.RecvMessage(0)
					if err != nil {
						pubErr := c.pubFail(parent, fmt.Errorf("manager.RecvMessage: %w", err))
						if pubErr != nil {
							c.errors = append(c.errors, fmt.Errorf("c.pubFail('%w'): %w", err, pubErr))
						}
						break
					}

					// the close parameter is set to true after reply the message back.
					// otherwise, the socket may be closed while the requester is waiting for a reply message.
					// it could leave to the app froze-up.
					reply := message.Reply{Status: message.OK, Parameters: key_value.Empty(), Message: ""}
					if err := c.reply(manager, reply); err != nil {
						failErr := fmt.Errorf("c.reply('manager'): %w", err)
						pubErr := c.pubFail(parent, failErr)
						if pubErr != nil {
							c.errors = append(c.errors, fmt.Errorf("c.pubFail('%w'): %w", failErr, pubErr))
						}
					}

					fmt.Printf("exiting from the instance\n")

					// mark as close, we don't exit straight from the loop,
					// because poller may be processing another signal.
					// so adding a close signal to the queue
					c.close = true
					c.status = CLOSED
				}
			}
		}

		fmt.Errorf("exited from poller loop\n")

		err = poller.RemoveBySocket(handler)
		if err != nil {
			pubErr := c.pubFail(parent, fmt.Errorf("poller.RemoveBySocket('handler'): %w", err))
			if pubErr != nil {
				// we add it to the error stack, but continue to clean out the rest
				// since removing from the poller won't affect the other socket operations.
				c.errors = append(c.errors, pubErr)
			}
		}

		err = poller.RemoveBySocket(manager)
		if err != nil {
			pubErr := c.pubFail(parent, fmt.Errorf("poller.RemoveBySocket('manager'): %w", err))
			if pubErr != nil {
				// we add it to the error stack, but continue to clean out the rest
				// since removing from the poller won't affect the other socket operations.
				c.errors = append(c.errors, pubErr)
			}
		}

		// if manager closing fails, then restart of this instance will throw an error,
		// since the manager is bound to the endpoint.
		//
		// one option is to remove the thread, and create a new instance with another id.
		err = manager.Close()
		if err != nil {
			c.errors = append(c.errors, fmt.Errorf("manager.Close: %w", err))
		}

		// if manager closing fails, then restart of this instance will throw an error.
		// see above manager.Close comment.
		err = handler.Close()
		if err != nil {
			c.errors = append(c.errors, fmt.Errorf("handler.Close: %w", err))
		}

		pubErr := c.pubStatus(parent, CLOSED)
		if pubErr != nil {
			// we add it to the error stack, but continue to clean out the rest
			// since removing from the poller won't affect the other socket operations.
			c.errors = append(c.errors, fmt.Errorf("c.pubStatis('CLOSED'): %w", pubErr))
		}

		err = parent.Close()
		if err != nil {
			c.errors = append(c.errors, fmt.Errorf("parent.Close: %w", err))
		}
	}(ready)

	return <-ready
}

func (c *Instance) processMessage(msgRaw []string, metadata map[string]string) (message.Reply, error) {
	// All request types derive from the basic request.
	// We first attempt to parse basic request from the raw message
	request, err := message.NewReqWithMeta(msgRaw, metadata)
	if err != nil {
		newErr := fmt.Errorf("message.NewReqWithMeta (msg len: %d): %w", len(msgRaw), err)

		return message.Reply{}, newErr
	}

	// Add the trace
	//if request.IsFirst() {
	//	request.SetUuid()
	//}
	//request.AddRequestStack(c.serviceUrl, c.config.Category, c.config.Instances[0].Id)

	handleInterface, depNames, err := route.Route(request.Command, *c.routes, *c.routeDeps)
	if err != nil {
		return request.Fail(fmt.Sprintf("route.Route(%s): %v", request.Command, err)), nil
	}

	depClients := route.FilterExtensionClients(depNames, *c.depClients)

	reply := route.Handle(request, handleInterface, depClients)

	// update the stack
	//if err = reply.SetStack(c.serviceUrl, c.config.Category, c.config.Instances[0].Id); err != nil {
	//	c.logger.Warn("failed to update the reply stack", "error", err)
	//}

	return *reply, nil
}
