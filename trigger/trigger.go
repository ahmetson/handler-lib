package trigger

import (
	"fmt"
	clientConfig "github.com/ahmetson/client-lib/config"
	"github.com/ahmetson/common-lib/data_type"
	"github.com/ahmetson/common-lib/data_type/key_value"
	"github.com/ahmetson/common-lib/message"
	"github.com/ahmetson/handler-lib/base"
	"github.com/ahmetson/handler-lib/config"
	"github.com/ahmetson/handler-lib/frontend"
	instances "github.com/ahmetson/handler-lib/instance_manager"
	"github.com/ahmetson/handler-lib/route"
	zmq "github.com/pebbe/zmq4"
)

const triggerType = config.ReplierType

type Trigger struct {
	*base.Handler
	socket       *zmq.Socket
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
		socket:       nil,
		broadcasting: data_type.NewQueue(),
	}
	return handler
}

// TriggerClient is the client parameters to trigger this handler
func (handler *Trigger) TriggerClient() *clientConfig.Client {
	handlerConfig := handler.Handler.Config()
	client := &clientConfig.Client{
		ServiceUrl: "",
		Id:         handlerConfig.Id,
		Port:       handlerConfig.Port,
		TargetType: config.SocketType(triggerType),
	}
	return client.UrlFunc(clientConfig.Url)
}

func (handler *Trigger) Close() error {
	if handler.closePub {
		return nil
	}
	handler.closePub = true

	return handler.Handler.Close()
}

// Route adds a route along with its handler to this handler
func (handler *Trigger) Route(_ string, _ interface{}, _ ...string) error {
	return fmt.Errorf("trigger doesn't support routing")
}

func (handler *Trigger) Config() *config.Trigger {
	baseConfig := handler.Handler.Config()
	trigger := config.Trigger{
		Handler:       baseConfig,
		BroadcastId:   handler.id,
		BroadcastPort: handler.port,
		BroadcastType: handler.handlerType,
	}
	return &trigger
}

// SetConfig adds the parameters of the handler from the config.
//
// Sets frontend's configuration as well.
func (handler *Trigger) SetConfig(trigger *config.Trigger) {
	// The broadcaster
	handler.port = trigger.BroadcastPort
	handler.id = trigger.BroadcastId
	handler.handlerType = trigger.BroadcastType

	// Todo change to the puller
	trigger.Handler.Type = triggerType

	handler.Handler.SetConfig(trigger.Handler)
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

	handler.socket = socket

	for {
		if handler.closePub {
			break
		}
		if handler.broadcasting.IsEmpty() {
			continue
		}

		req := handler.broadcasting.Pop().(message.Request)
		reqStr, err := req.String()
		if err != nil {
			handler.status = fmt.Sprintf("socket.SendMessageDontWait: %v", err)
			break
		}
		_, err = socket.SendMessageDontwait(reqStr)
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
	handler.socket = nil
}

func (handler *Trigger) onTrigger(req message.Request) message.Reply {
	if handler.broadcasting.IsFull() {
		return req.Fail("broadcasting queue full")
	}

	handler.broadcasting.Push(req)

	return req.Ok(key_value.Empty())
}

// Start the trigger directly, not by goroutine.
//
// The Trigger-able handlers can have only one instance
func (handler *Trigger) Start() error {
	m := handler.Handler

	if m.Config() == nil {
		return fmt.Errorf("configuration not set")
	}
	if !config.CanTrigger(handler.handlerType) {
		return fmt.Errorf("the '%s' handler type in configuration is not triggerable", handler.handlerType)
	}
	if m.Manager == nil {
		return fmt.Errorf("handler manager not set. call SetConfig and SetLogger first")
	}

	if err := m.Route(route.Any, handler.onTrigger); err != nil {
		return fmt.Errorf("handler.Route: %w", err)
	}

	// add a routing that redirects the messages to the trigger
	onAddInstance := func(req message.Request) message.Reply {
		if len(m.InstanceManager.Instances()) != 0 {
			return req.Fail(fmt.Sprintf("only one instance allowed in sync replier"))
		}

		instanceId, err := m.InstanceManager.AddInstance(m.Config().Type, &m.Routes, &m.RouteDeps, &m.DepClients)
		if err != nil {
			return req.Fail(fmt.Sprintf("instanceManager.AddInstance(%s): %v", m.Config().Type, err))
		}

		params := key_value.Empty().Set("instance_id", instanceId)
		return req.Ok(params)
	}
	onClose := func(req message.Request) message.Reply {
		part, err := req.Parameters.GetString("part")
		if err != nil {
			return req.Fail(fmt.Sprintf("req.Parameters.GetString('part'): %v", err))
		}

		if part == "frontend" {
			if m.Frontend.Status() != frontend.RUNNING {
				return req.Fail("frontend not running")
			} else {
				if err := m.Frontend.Close(); err != nil {
					return req.Fail(fmt.Sprintf("failed to close the frontend: %v", err))
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

		if part == "frontend" {
			if m.Frontend.Status() == frontend.RUNNING {
				return req.Fail("frontend running")
			} else {
				go m.Frontend.Run()
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
			Set("queue_length", m.Frontend.QueueLen()).
			Set("processing_length", m.Frontend.ProcessingLen()).
			Set("broadcasting_length", handler.broadcasting.Len())
		return req.Ok(params)
	}

	onParts := func(req message.Request) message.Reply {
		parts := []string{
			"frontend",
			"instance_manager",
			"broadcaster",
		}
		messageTypes := []string{
			"queue_length",
			"processing_length",
			"broadcasting_length",
		}

		params := key_value.Empty().
			Set("parts", parts).
			Set("message_types", messageTypes)

		return req.Ok(params)
	}

	if err := m.Manager.Route(config.AddInstance, onAddInstance); err != nil {
		return fmt.Errorf("overwriting handler manager 'add_instance' failed: %w", err)
	}
	if err := m.Manager.Route(config.ClosePart, onClose); err != nil {
		return fmt.Errorf("overwriting handler manager 'close' failed: %w", err)
	}
	if err := m.Manager.Route(config.RunPart, onRunPart); err != nil {
		return fmt.Errorf("overwriting handler manager 'run' failed: %w", err)
	}
	if err := m.Manager.Route(config.MessageAmount, onMessageAmount); err != nil {
		return fmt.Errorf("overwriting handler manager 'message_amount' failed: %w", err)
	}
	if err := m.Manager.Route(config.Parts, onParts); err != nil {
		return fmt.Errorf("overwriting handler manager 'parts' failed: %w", err)
	}

	go handler.runBroadcaster()

	return m.Start()
}
