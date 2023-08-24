// Package base keeps the generic Handler.
// It's not intended to be used independently.
// Other handlers should be defined based on this handler
package base

import (
	"fmt"
	"github.com/ahmetson/client-lib"
	clientConfig "github.com/ahmetson/client-lib/config"
	"github.com/ahmetson/common-lib/data_type/key_value"
	"github.com/ahmetson/handler-lib/config"
	"github.com/ahmetson/handler-lib/instance_manager"
	"github.com/ahmetson/handler-lib/reactor"
	"github.com/ahmetson/handler-lib/route"
	"github.com/ahmetson/log-lib"
	"github.com/ahmetson/os-lib/net"
	"github.com/ahmetson/os-lib/process"
	"slices"

	"github.com/ahmetson/common-lib/message"
	zmq "github.com/pebbe/zmq4"
)

// The Handler is the socket wrapper for the clientConfig.
type Handler struct {
	config              *config.Handler
	socket              *zmq.Socket
	logger              *log.Logger
	routes              key_value.KeyValue
	routeDeps           key_value.KeyValue
	depIds              []string
	depConfigs          key_value.KeyValue
	depClients          client.Clients
	reactor             *reactor.Reactor
	instanceManager     *instance_manager.Parent
	instanceManagerRuns bool
}

// New handler
func New() *Handler {
	return &Handler{
		logger:              nil,
		routes:              key_value.Empty(),
		routeDeps:           key_value.Empty(),
		depIds:              make([]string, 0),
		depConfigs:          key_value.Empty(),
		depClients:          key_value.Empty(),
		reactor:             reactor.New(),
		instanceManager:     nil,
		instanceManagerRuns: false,
	}
}

// SetConfig adds the parameters of the server from the config.
func (c *Handler) SetConfig(controller *config.Handler) {
	c.config = controller
	c.reactor.SetConfig(controller)
}

// SetLogger sets the logger.
func (c *Handler) SetLogger(parent *log.Logger) error {
	if c.config == nil {
		return fmt.Errorf("missing configuration")
	}
	logger := parent.Child(c.config.Id)
	c.logger = logger

	c.instanceManager = instance_manager.New(c.config.Id, c.logger)
	c.reactor.SetInstanceManager(c.instanceManager)

	return nil
}

// AddDepByService adds the config of the dependency. Intended to be called by Service not by developer
func (c *Handler) AddDepByService(dep *clientConfig.Client) error {
	if c.AddedDepByService(dep.Id) {
		return fmt.Errorf("dependency configuration already added")
	}

	if !slices.Contains(c.depIds, dep.Id) {
		return fmt.Errorf("no handler depends on '%s'", dep.Id)
	}

	c.depConfigs.Set(dep.Id, dep)
	return nil
}

// AddedDepByService returns true if the configuration exists
func (c *Handler) AddedDepByService(id string) bool {
	return c.depConfigs.Exist(id) == nil
}

// addDep adds the dependency id required by one of the routes.
// Already added dependency id skipped.
func (c *Handler) addDep(id string) {
	if !slices.Contains(c.depIds, id) {
		c.depIds = append(c.depIds, id)
	}
}

// DepIds return the list of extension names required by this server.
func (c *Handler) DepIds() []string {
	return c.depIds
}

// A reply sends to the caller the message.
//
// If a server doesn't support replying (for example, PULL server),
// then it returns success.
func (c *Handler) reply(socket *zmq.Socket, message message.Reply) error {
	if !config.CanReply(c.config.Type) {
		return nil
	}

	reply, _ := message.String()
	if _, err := socket.SendMessage(reply); err != nil {
		return fmt.Errorf("recv error replying error %w" + err.Error())
	}

	return nil
}

// Calls server.reply() with the error message.
func (c *Handler) replyError(socket *zmq.Socket, err error) error {
	request := message.Request{}
	return c.reply(socket, request.Fail(err.Error()))
}

// Route adds a route along with its handler to this server
func (c *Handler) Route(cmd string, handle any, depIds ...string) error {
	if !route.IsHandleFunc(handle) {
		return fmt.Errorf("handle is not a valid handle function")
	}
	depAmount := route.DepAmount(handle)
	if !route.IsHandleFuncWithDeps(handle, len(depIds)) {
		return fmt.Errorf("the '%s' command handler requires %d dependencies, but route has %d dependencies", cmd, depAmount, len(depIds))
	}

	if err := c.routes.Exist(cmd); err == nil {
		return nil
	}

	for _, dep := range depIds {
		c.addDep(dep)
	}

	c.routes.Set(cmd, handle)
	if len(depIds) > 0 {
		c.routeDeps.Set(cmd, depIds)
	}

	return nil
}

// depConfigsAdded checks that the required depClients are added into the server.
// If no depClients are added by calling server.addDep(), then it will return nil.
func (c *Handler) depConfigsAdded() error {
	if len(c.depIds) != len(c.depConfigs) {
		return fmt.Errorf("required dependencies and configurations are not matching")
	}
	for _, id := range c.depIds {
		if err := c.depConfigs.Exist(id); err != nil {
			return fmt.Errorf("'%s' dependency configuration not added", id)
		}
	}

	return nil
}

// Type returns the handler type. If the configuration is not set, returns config.UnknownType.
func (c *Handler) Type() config.HandlerType {
	if c.config == nil {
		return config.UnknownType
	}
	return c.config.Type
}

// initDepClients will set up the extension clients for this server.
// It will be called by c.Run(), automatically.
//
// The reason for why we call it by c.Run() is due to the thread-safety.
//
// The server is intended to be called as the goroutine. And if the sockets
// are not initiated within the same goroutine, then zeromq doesn't guarantee the socket work
// as it's intended.
func (c *Handler) initDepClients() error {
	for _, depInterface := range c.depConfigs {
		depConfig := depInterface.(*clientConfig.Client)

		if err := c.depClients.Exist(depConfig.Id); err == nil {
			return fmt.Errorf("depClients.Exist('%s')", depConfig.Id)
		}

		depClient, err := client.NewReq(depConfig.Id, depConfig.Port, c.logger)
		if err != nil {
			return fmt.Errorf("client.NewReq('%s', '%d'): %w", depConfig.Id, depConfig.Port, err)
		}
		c.depClients.Set(depConfig.Id, depClient)
	}

	return nil
}

// Close the handler
func (c *Handler) Close() error {
	if c.instanceManager.Status() == instance_manager.Running {
		c.instanceManager.Close()
	}
	if c.reactor.Status() == reactor.RUNNING {
		if err := c.reactor.Close(); err != nil {
			return fmt.Errorf("c.reactor.Close: %w", err)
		}
	}

	return nil
}

// Runs the instance manager and listen to it's events
func (c *Handler) runInstanceManager() {
	socket, err := zmq.NewSocket(zmq.SUB)
	if err != nil {
		c.logger.Warn("zmq.NewSocket", "id", c.config.Id, "function", "runInstanceManager", "error", err)
		return
	}

	if err := socket.SetSubscribe(""); err != nil {
		c.logger.Warn("set subscriber", "id", c.config.Id, "function", "runInstanceManager", "error", err)
		return
	}

	url := config.InstanceManagerEventUrl(c.config.Id)
	err = socket.Connect(url)
	if err != nil {
		c.logger.Warn("eventSocket.Connect", "id", c.config.Id, "function", "runInstanceManager", "url", url, "error", err)
		return
	}
	c.instanceManagerRuns = true

	go c.instanceManager.Run()

	// The first Instance created by handler when the instance manager is ready.
	firstInstance := false
	// Verify that the first instance was added.
	instanceId := ""

	for {
		raw, err := socket.RecvMessage(0)
		if err != nil {
			c.logger.Warn("eventSocket.RecvMessage", "id", c.config.Id, "error", err)
			break
		}

		req, err := message.NewReq(raw)
		if err != nil {
			c.logger.Warn("eventSocket: convert raw to message", "id", c.config.Id, "message", raw, "error", err)
			continue
		}

		if req.Command == instance_manager.EventReady {
			if !firstInstance {
				instanceId, err = c.instanceManager.AddInstance(c.config.Type, &c.routes, &c.routeDeps, &c.depClients)
				if err != nil {
					c.logger.Warn("instanceManager.AddInstance", "id", c.config.Id, "event", req.Command, "type", c.config.Type, "error", err)
					continue
				}
				firstInstance = true
			}
		} else if req.Command == instance_manager.EventInstanceAdded {
			if firstInstance && len(instanceId) > 0 {
				addedInstanceId, err := req.Parameters.GetString("id")
				if err != nil {
					c.logger.Warn("req.Parameters.GetString('id')", "id", c.config.Id, "event", req.Command, "instanceId", instanceId, "error", err)
					continue
				}
				if addedInstanceId != instanceId {
					continue
				} else {
					c.logger.Info("instance was added", "id", c.config.Id, "event", req.Command, "instanceId", instanceId)
					instanceId = ""
				}
			}
		} else if req.Command == instance_manager.EventError {
			errMsg, err := req.Parameters.GetString("message")
			if err != nil {
				c.logger.Warn("req.Parameters.GetString('message')", "id", c.config.Id, "event", req.Command, "error", err)
				continue
			}

			c.logger.Warn("instance manager exited with an error", "id", c.config.Id, "error", errMsg)
			break
		} else if req.Command == instance_manager.EventIdle {
			close, _ := req.Parameters.GetBoolean("close")
			c.logger.Warn("instance manager is idle", "id", c.config.Id, "close signal received?", close)
			if close {
				firstInstance = false
			}
		} else {
			c.logger.Warn("unhandled instance manager event", "id", c.config.Id, "event", req.Command, "parameters", req.Parameters)
		}
	}

	err = socket.Close()
	if err != nil {
		c.logger.Warn("failed to close instance manager sub", "id", c.config.Id, "error", err)
	}

	c.instanceManagerRuns = false
}

// Start the handler directly, not by goroutine
func (c *Handler) Start() error {
	if c.config == nil {
		return fmt.Errorf("configuration not set")
	}
	if err := c.depConfigsAdded(); err != nil {
		return fmt.Errorf("depConfigsAdded: %w", err)
	}
	if c.instanceManager == nil {
		return fmt.Errorf("instance manager not set")
	}
	if c.reactor == nil {
		return fmt.Errorf("reactor not set")
	}
	if err := c.initDepClients(); err != nil {
		return fmt.Errorf("initDepClients: %w", err)
	}

	// Adding the first instance manager
	go c.reactor.Run()
	go c.runInstanceManager()

	return nil
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
						err = fmt.Errorf("operating system uses it for another clientConfig. pid=%d", pid)
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
