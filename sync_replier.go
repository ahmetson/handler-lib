package handler

import (
	"fmt"
	"github.com/ahmetson/common-lib/message"
	"github.com/ahmetson/handler-lib/route"
	zmq "github.com/pebbe/zmq4"
)

func (c *Handler) prepare() error {
	if err := c.extensionsAdded(); err != nil {
		return fmt.Errorf("extensionsAdded: %w", err)
	}
	if err := c.initExtensionClients(); err != nil {
		return fmt.Errorf("initExtensionClients: %w", err)
	}
	if c.config == nil || len(c.config.Instances) == 0 {
		return fmt.Errorf("server doesn't have the config or instances are missing")
	}

	return nil
}

func (c *Handler) processMessage(msgRaw []string, metadata map[string]string) (message.Reply, error) {
	// All request types derive from the basic request.
	// We first attempt to parse basic request from the raw message
	request, err := message.NewReqWithMeta(msgRaw, metadata)
	if err != nil {
		newErr := fmt.Errorf("message.ParseRequest: %w", err)

		return message.Reply{}, newErr
	}

	// Add the trace
	if request.IsFirst() {
		request.SetUuid()
	}
	//request.AddRequestStack(c.serviceUrl, c.config.Category, c.config.Instances[0].Id)

	handleInterface, handleDeps, err := route.HandleFunc(request.Command, c.routes, c.routeDeps)
	if err != nil {
		return request.Fail(fmt.Sprintf("route.HandleFunc(%s)", request.Command)), nil
	}

	reply := route.HandleReq(request, handleInterface, handleDeps, c.extensions)

	// update the stack
	//if err = reply.SetStack(c.serviceUrl, c.config.Category, c.config.Instances[0].Id); err != nil {
	//	c.logger.Warn("failed to update the reply stack", "error", err)
	//}

	return *reply, nil
}

func (c *Handler) Run() error {
	if c.config == nil {
		return fmt.Errorf("missing config")
	}

	sock, err := zmq.NewSocket(getSocket(c.controllerType))
	if err != nil {
		return fmt.Errorf("zmq.NewSocket: %w", err)
	}
	c.socket = sock

	sockUrl := url(c.config.Instances[0].Id, c.config.Instances[0].Port)
	if err := bind(c.socket, sockUrl, c.config.Instances[0].Port); err != nil {
		return fmt.Errorf(`bind("%s"): %w`, c.config.Instances[0].ControllerCategory, err)
	}

	poller := zmq.NewPoller()
	poller.Add(c.socket, zmq.POLLIN)

	for {
		sockets, err := poller.Poll(-1)
		if err != nil {
			newErr := fmt.Errorf("poller.Poll(%s): %w", c.config.Category, err)
			return newErr
		}

		if len(sockets) > 0 {
			data, _, err := c.socket.RecvMessageWithMetadata(0, requiredMetadata()...)
			if err != nil {
				newErr := fmt.Errorf("socket.recvMessageWithMetadata: %w", err)
				if err := c.replyError(c.socket, newErr); err != nil {
					return err
				}
				return newErr
			}

			c.logger.Info("message received", "messages", data)

			if err := c.reply(c.socket, message.Reply{}); err != nil {
				c.logger.Fatal("failed")
			}
		}
	}
}

//
//func (c *Handler) Run() error {
//	var err error
//	if err := c.prepare(); err != nil {
//		return fmt.Errorf("server.prepare: %w", err)
//	}
//
//	// Socket to talk to clients
//	c.socket, err = zmq.NewSocket(zmq.REP)
//	if err != nil {
//		return fmt.Errorf("zmq.NewSocket: %w", err)
//	}
//
//	// if secure and not inproc
//	// then we add the domain name of server to the security layer,
//	//
//	// then any pass-listing users will be sent there.
//	c.logger.Warn("todo", "todo 1", "make sure that all ports are different")
//
//	url := url(c.config.Instances[0].ControllerCategory, c.config.Instances[0].Port)
//	c.logger.Warn("config.Instances[0] is hardcoded. Create multiple instances", "url", url, "name", c.config.Instances[0].ControllerCategory)
//
//	if err := bind(c.socket, url, c.config.Instances[0].Port); err != nil {
//		return fmt.Errorf(`bind("%s"): %w`, c.config.Instances[0].ControllerCategory, err)
//	}
//
//	poller := zmq.NewPoller()
//	poller.Add(c.socket, zmq.POLLIN)
//
//	for {
//		sockets, err := poller.Poll(-1)
//		if err != nil {
//			newErr := fmt.Errorf("poller.Poll(%s): %w", c.config.Category, err)
//			return newErr
//		}
//
//		if len(sockets) > 0 {
//			msgRaw, metadata, err := c.socket.RecvMessageWithMetadata(0, requiredMetadata()...)
//			if err != nil {
//				newErr := fmt.Errorf("socket.recvMessageWithMetadata: %w", err)
//				if err := c.replyError(c.socket, newErr); err != nil {
//					return err
//				}
//				return newErr
//			}
//
//			reply, err := c.processMessage(msgRaw, metadata)
//			if err != nil {
//				if err := c.replyError(c.socket, err); err != nil {
//					return fmt.Errorf("replyError: %w", err)
//				}
//			} else {
//				if err := c.reply(c.socket, reply); err != nil {
//					return fmt.Errorf("reply: %w: ", err)
//				}
//			}
//		}
//	}
//}
