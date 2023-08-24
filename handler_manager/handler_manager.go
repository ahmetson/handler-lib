// Package handler_manager creates a socket that manages the handler
package handler_manager

import (
	"fmt"
	"github.com/ahmetson/common-lib/data_type/key_value"
	"github.com/ahmetson/common-lib/message"
	"github.com/ahmetson/handler-lib/config"
	instances "github.com/ahmetson/handler-lib/instance_manager"
	"github.com/ahmetson/handler-lib/reactor"
	"github.com/ahmetson/handler-lib/route"
	zmq "github.com/pebbe/zmq4"
)

const (
	Incomplete  = "incomplete"
	Ready       = "ready"
	SocketIdle  = "idle"
	SocketReady = "ready"
)

type HandlerManager struct {
	reactor            *reactor.Reactor
	instanceManager    *instances.Parent
	runInstanceManager func()
	config             *config.Handler
	routes             key_value.KeyValue
	routeDeps          key_value.KeyValue
	depClients         key_value.KeyValue
	status             string // It's the socket status, not the handler status
	close              bool
}

// New handler manager
func New(reactor *reactor.Reactor, parent *instances.Parent, runInstanceManager func()) *HandlerManager {
	return &HandlerManager{
		reactor:            reactor,
		instanceManager:    parent,
		runInstanceManager: runInstanceManager,
		routes:             key_value.Empty(),
		routeDeps:          key_value.Empty(),
		depClients:         key_value.Empty(),
		status:             SocketIdle,
	}
}

func (m *HandlerManager) SetConfig(config *config.Handler) {
	m.config = config
}

func (m *HandlerManager) setRoutes() {
	// Requesting status
	onStatus := func(req message.Request) message.Reply {
		reactorStatus := m.reactor.Status()
		instanceStatus := m.instanceManager.Status()

		params := key_value.Empty()

		if reactorStatus == reactor.RUNNING && instanceStatus == instances.Running {
			params.Set("status", Ready)
		} else {
			parts := key_value.Empty().
				Set("reactor", reactorStatus).
				Set("instance_manager", instanceStatus)

			params.Set("status", Incomplete).
				Set("parts", parts)
		}

		return req.Ok(params)
	}
	// Stop one of the parts
	// Either: reactor or instance manager
	onClose := func(req message.Request) message.Reply {
		part, err := req.Parameters.GetString("part")
		if err != nil {
			return req.Fail(fmt.Sprintf("req.Parameters.GetString('part'): %v", err))
		}

		if part == "reactor" {
			if m.reactor.Status() != reactor.RUNNING {
				return req.Fail("reactor not running")
			} else {
				if err := m.reactor.Close(); err != nil {
					return req.Fail(fmt.Sprintf("failed to close the reactor: %v", err))
				}
				return req.Ok(key_value.Empty())
			}
		} else if part == "instance_manager" {
			if m.instanceManager.Status() != instances.Running {
				return req.Fail("instance manager not running")
			} else {
				m.instanceManager.Close()
				return req.Ok(key_value.Empty())
			}
		} else {
			return req.Fail(fmt.Sprintf("unknown part '%s' to stop", part))
		}
	}

	onRun := func(req message.Request) message.Reply {
		part, err := req.Parameters.GetString("part")
		if err != nil {
			return req.Fail(fmt.Sprintf("req.Parameters.GetString('part'): %v", err))
		}

		if part == "reactor" {
			if m.reactor.Status() == reactor.RUNNING {
				return req.Fail("reactor running")
			} else {
				go m.reactor.Run()
				return req.Ok(key_value.Empty())
			}
		} else if part == "instance_manager" {
			if m.instanceManager.Status() == instances.Running {
				return req.Fail("instance manager running")
			} else {
				go m.runInstanceManager()
				return req.Ok(key_value.Empty())
			}
		} else {
			return req.Fail(fmt.Sprintf("unknown part '%s' to stop", part))
		}
	}

	onInstanceAmount := func(req message.Request) message.Reply {
		instanceAmount := len(m.instanceManager.Instances())
		return req.Ok(key_value.Empty().Set("instance_amount", instanceAmount))
	}

	onMessageAmount := func(req message.Request) message.Reply {
		return req.Ok(key_value.Empty().Set("message_amount", m.reactor.QueueLen()))
	}

	m.routes.Set("status", onStatus)
	m.routes.Set("close", onClose)
	m.routes.Set("run", onRun)
	m.routes.Set("instance_amount", onInstanceAmount)
	m.routes.Set("message_amount", onMessageAmount)
}

func (m *HandlerManager) Close() {
	m.close = true
}

func (m *HandlerManager) Run() error {
	if m.config == nil {
		return fmt.Errorf("no config")
	}

	manager, err := zmq.NewSocket(zmq.ROUTER)
	if err != nil {
		return fmt.Errorf("zmq.NewSocket: %w", err)
	}

	url := config.ManagerUrl(m.config.Id)
	err = manager.Bind(url)
	if err != nil {
		return fmt.Errorf("manager.Bind('%s'): %w", url, err)
	}

	m.setRoutes()

	var loopErr error

	poller := zmq.NewPoller()
	poller.Add(manager, zmq.POLLIN)

	m.status = SocketReady

	for {
		if m.close {
			err = poller.RemoveBySocket(manager)
			if err != nil {
				loopErr = fmt.Errorf("remove manager: %w", err)
			}
			break
		}

		sockets, err := poller.Poll(0)
		if err != nil {
			loopErr = fmt.Errorf("poller.Poll: %w", err)
			break
		}

		if len(sockets) == 0 {
			continue
		}

		raw, err := manager.RecvMessage(0)
		if err != nil {
			loopErr = fmt.Errorf("manager.RecvMessage")
			break
		}

		req, err := message.NewReq(raw)
		if err != nil {
			fmt.Printf("failed to parse request: %v\n", err)
			continue
		}

		handleInterface, depNames, err := route.Route(req.Command, m.routes, m.routeDeps)
		if err != nil {
			reply := req.Fail(fmt.Sprintf("route.Route(%s): %v", req.Command, err))
			replyStr, err := reply.String()
			if err != nil {
				reply := req.Fail(fmt.Sprintf("failed to convert reply [%v] to string", reply))
				replyStr, err := reply.String()
				if err != nil {
					fmt.Printf("failed to convert request '%v' to reply '%v' to string: %v\n", req, reply, err)
					continue
				}
				_, err = manager.SendMessage(raw[0], raw[1], replyStr)
				if err != nil {
					fmt.Printf("failed to reply back for request '%v' the '%v' string: %v\n", req, replyStr, err)
				}
			} else {
				_, err = manager.SendMessage(raw[0], raw[1], replyStr)
				if err != nil {
					fmt.Printf("failed to reply back for request '%v' the '%v' string: %v\n", req, replyStr, err)
				}
			}
			continue
		}

		depClients := route.FilterExtensionClients(depNames, m.depClients)

		reply := route.Handle(req, handleInterface, depClients)
		replyStr, err := reply.String()
		if err != nil {
			reply := req.Fail(fmt.Sprintf("failed to convert handle reply [%v] to string", reply))
			replyStr, err := reply.String()
			if err != nil {
				fmt.Printf("failed to convert request '%v' to reply '%v' to string: %v\n", req, reply, err)
				continue
			}
			_, err = manager.SendMessage(raw[0], raw[1], replyStr)
			if err != nil {
				fmt.Printf("failed to reply back for request '%v' the '%v' string: %v\n", req, replyStr, err)
				continue
			}
		} else {
			_, err = manager.SendMessage(raw[0], raw[1], replyStr)
			if err != nil {
				fmt.Printf("failed to reply back for request '%v' the '%v' string: %v\n", req, replyStr, err)
				continue
			}
		}
	}

	m.status = SocketIdle

	closeErr := manager.Close()
	if closeErr != nil {
		return fmt.Errorf("manager.Close: %w", err)
	}

	return loopErr
}