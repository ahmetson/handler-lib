package trigger

import (
	"fmt"
	clientConfig "github.com/ahmetson/client-lib/config"
	"github.com/ahmetson/common-lib/data_type"
	"github.com/ahmetson/common-lib/data_type/key_value"
	"github.com/ahmetson/common-lib/message"
	"github.com/ahmetson/handler-lib/base"
	"github.com/ahmetson/handler-lib/config"
	instances "github.com/ahmetson/handler-lib/instance_manager"
	"github.com/ahmetson/handler-lib/reactor"
	"github.com/ahmetson/handler-lib/route"
	zmq "github.com/pebbe/zmq4"
)

const triggerType = config.ReplierType

type Trigger struct {
	*base.Handler
	closePub     bool
	port         uint64
	id           string
	status       string
	handlerType  config.HandlerType
	broadcasting *data_type.Queue
}

// New trigger-able handler
func New() *Trigger {
	handler := &Trigger{
		Handler:      base.New(),
		closePub:     false,
		broadcasting: data_type.NewQueue(),
	}
	return handler
}

// TriggerClient is the client parameters to trigger this handler
func (handler *Trigger) TriggerClient() *clientConfig.Client {
	handlerConfig := handler.Handler.Config
	client := &clientConfig.Client{
		ServiceUrl: "",
		Id:         handlerConfig.Id,
		Port:       handlerConfig.Port,
		TargetType: config.SocketType(triggerType),
	}
	return client.UrlFunc(config.ExternalUrlByClient)
}

func (handler *Trigger) Close() error {
	if handler.closePub {
		return nil
	}
	handler.closePub = true

	return handler.Handler.Close()
}

// Route adds a route along with its handler to this server
func (handler *Trigger) Route(string, any, ...string) error {
	return fmt.Errorf("trigger doesn't support routing")
}

// SetConfig adds the parameters of the server from the Config.
//
// Sets reactor's configuration as well.
func (handler *Trigger) SetConfig(controller *config.Trigger) {
	// The broadcaster
	handler.port = controller.BroadcastPort
	handler.id = controller.BroadcastId
	handler.handlerType = controller.BroadcastType

	// Todo change to the puller
	controller.Handler.Type = triggerType

	handler.Handler.SetConfig(controller.Handler)
}

// runBroadcaster creates a socket that will be linked by the user
func (handler *Trigger) runBroadcaster() {
	socket, err := zmq.NewSocket(config.SocketType(handler.handlerType))
	if err != nil {
		handler.status = fmt.Sprintf("new_socket('%s'): %v", handler.handlerType, err)
		return
	}

	pubUrl := config.ExternalUrl(handler.id, handler.port)
	err = socket.Bind(pubUrl)
	if err != nil {
		handler.status = fmt.Sprintf("socket.Bind('%s'): %v", pubUrl, err)
	}

	for {
		if handler.closePub {
			break
		}
		if handler.broadcasting.IsEmpty() {
			continue
		}

		messages := handler.broadcasting.Pop().([]string)
		_, err := socket.SendMessageDontwait(messages)
		if err != nil {
			handler.status = fmt.Sprintf("socket.SendMessageDontWait: %v", err)
			break
		}
	}

	handler.closePub = false
	err = socket.Close()
	if err != nil {
		handler.status = fmt.Sprintf("socket.Close: %v", err)
		return
	}

	handler.status = ""
}

func (handler *Trigger) onTrigger(req message.Request) message.Reply {
	if handler.broadcasting.IsFull() {
		return req.Fail("broadcasting queue full")
	}

	reqStr, err := req.String()
	if err != nil {
		return req.Fail(fmt.Sprintf("request.String: %v", err))
	}
	handler.broadcasting.Push(reqStr)

	return req.Ok(key_value.Empty())
}

// Start the trigger directly, not by goroutine.
//
// The Trigger-able handlers can have only one instance
func (handler *Trigger) Start() error {
	m := handler.Handler

	if m.Config == nil {
		return fmt.Errorf("configuration not set")
	}
	if !config.CanTrigger(handler.handlerType) {
		return fmt.Errorf("the '%s' handler type in configuration is not triggerable", handler.handlerType)
	}

	if err := m.Route(route.Any, handler.onTrigger); err != nil {
		return fmt.Errorf("handler.Route: %w", err)
	}

	// add a routing that redirects the messages to the trigger
	onAddInstance := func(req message.Request) message.Reply {
		if len(m.InstanceManager.Instances()) != 0 {
			return req.Fail(fmt.Sprintf("only one instance allowed in sync replier"))
		}

		instanceId, err := m.InstanceManager.AddInstance(m.Config.Type, &m.Routes, &m.RouteDeps, &m.DepClients)
		if err != nil {
			return req.Fail(fmt.Sprintf("instanceManager.AddInstance(%s): %v", m.Config.Type, err))
		}

		params := key_value.Empty().Set("instance_id", instanceId)
		return req.Ok(params)
	}

	onClose := func(req message.Request) message.Reply {
		part, err := req.Parameters.GetString("part")
		if err != nil {
			return req.Fail(fmt.Sprintf("req.Parameters.GetString('part'): %v", err))
		}

		if part == "reactor" {
			if m.Reactor.Status() != reactor.RUNNING {
				return req.Fail("reactor not running")
			} else {
				if err := m.Reactor.Close(); err != nil {
					return req.Fail(fmt.Sprintf("failed to close the reactor: %v", err))
				}
				return req.Ok(key_value.Empty())
			}
		} else if part == "instance_manager" {
			if m.InstanceManager.Status() != instances.Running {
				return req.Fail("instance manager not running")
			} else {
				m.InstanceManager.Close()
				return req.Ok(key_value.Empty())
			}
		} else if part == "broadcaster" {
			handler.closePub = true
			return req.Ok(key_value.Empty())
		} else {
			return req.Fail(fmt.Sprintf("unknown part '%s' to stop", part))
		}
	}

	onRunPart := func(req message.Request) message.Reply {
		part, err := req.Parameters.GetString("part")
		if err != nil {
			return req.Fail(fmt.Sprintf("req.Parameters.GetString('part'): %v", err))
		}

		if part == "reactor" {
			if m.Reactor.Status() == reactor.RUNNING {
				return req.Fail("reactor running")
			} else {
				go m.Reactor.Run()
				return req.Ok(key_value.Empty())
			}
		} else if part == "instance_manager" {
			if m.InstanceManager.Status() == instances.Running {
				return req.Fail("instance manager running")
			} else {
				go m.RunInstanceManager()
				return req.Ok(key_value.Empty())
			}
		} else if part == "broadcaster" {
			go handler.runBroadcaster()
			return req.Ok(key_value.Empty())
		} else {
			return req.Fail(fmt.Sprintf("unknown part '%s' to stop", part))
		}
	}
	onMessageAmount := func(req message.Request) message.Reply {
		params := key_value.Empty().
			Set("queue_length", m.Reactor.QueueLen()).
			Set("processing_length", m.Reactor.ProcessingLen()).
			Set("broadcasting_length", handler.broadcasting.Len())
		return req.Ok(params)
	}

	if err := m.Manager.Route("add_instance", onAddInstance); err != nil {
		return fmt.Errorf("overwriting handler manager 'add_instance' failed: %w", err)
	}
	if err := m.Manager.Route("close_part", onClose); err != nil {
		return fmt.Errorf("overwriting handler manager 'close' failed: %w", err)
	}
	if err := m.Manager.Route("run_part", onRunPart); err != nil {
		return fmt.Errorf("overwriting handler manager 'run' failed: %w", err)
	}
	if err := m.Manager.Route("message_amount", onMessageAmount); err != nil {
		return fmt.Errorf("overwriting handler manager 'message_amount' failed: %w", err)
	}

	go handler.runBroadcaster()

	return m.Start()
}
