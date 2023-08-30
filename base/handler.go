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
	"github.com/ahmetson/handler-lib/handler_manager"
	"github.com/ahmetson/handler-lib/instance_manager"
	"github.com/ahmetson/handler-lib/reactor"
	"github.com/ahmetson/handler-lib/route"
	"github.com/ahmetson/log-lib"
	"slices"

	"github.com/ahmetson/common-lib/message"
	zmq "github.com/pebbe/zmq4"
)

// The Handler is the socket wrapper for the clientConfig.
type Handler struct {
	Config              *config.Handler
	socket              *zmq.Socket
	logger              *log.Logger
	Routes              key_value.KeyValue
	RouteDeps           key_value.KeyValue
	depIds              []string
	depConfigs          key_value.KeyValue
	DepClients          key_value.KeyValue
	Reactor             *reactor.Reactor
	InstanceManager     *instance_manager.Parent
	instanceManagerRuns bool
	Manager             *handler_manager.HandlerManager
	status              string
}

// New handler
func New() *Handler {
	return &Handler{
		logger:              nil,
		Routes:              key_value.Empty(),
		RouteDeps:           key_value.Empty(),
		depIds:              make([]string, 0),
		depConfigs:          key_value.Empty(),
		DepClients:          key_value.Empty(),
		Reactor:             reactor.New(),
		InstanceManager:     nil,
		instanceManagerRuns: false,
		Manager:             nil,
		status:              "",
	}
}

// SetConfig adds the parameters of the server from the Config.
//
// Sets Reactor's configuration as well.
func (c *Handler) SetConfig(controller *config.Handler) {
	c.Config = controller
	c.Reactor.SetConfig(controller)
}

// SetLogger sets the logger (depends on configuration).
//
// Creates instance Manager.
//
// Creates handler Manager.
func (c *Handler) SetLogger(parent *log.Logger) error {
	if c.Config == nil {
		return fmt.Errorf("missing configuration")
	}
	logger := parent.Child(c.Config.Id)
	c.logger = logger

	c.InstanceManager = instance_manager.New(c.Config.Id, c.logger)
	c.Reactor.SetInstanceManager(c.InstanceManager)

	c.Manager = handler_manager.New(c.Reactor, c.InstanceManager, c.RunInstanceManager)
	c.Manager.SetConfig(c.Config)

	return nil
}

// AddDepByService adds the Config of the dependency. Intended to be called by Service not by developer
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

// addDep adds the dependency id required by one of the Routes.
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
	if !config.CanReply(c.Config.Type) {
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

	if err := c.Routes.Exist(cmd); err == nil {
		return nil
	}

	for _, dep := range depIds {
		c.addDep(dep)
	}

	c.Routes.Set(cmd, handle)
	if len(depIds) > 0 {
		c.RouteDeps.Set(cmd, depIds)
	}

	return nil
}

// depConfigsAdded checks that the required DepClients are added into the server.
// If no DepClients are added by calling server.addDep(), then it will return nil.
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
	if c.Config == nil {
		return config.UnknownType
	}
	return c.Config.Type
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

		if err := c.DepClients.Exist(depConfig.Id); err == nil {
			return fmt.Errorf("DepClients.Exist('%s')", depConfig.Id)
		}

		depClient, err := client.New(depConfig)
		if err != nil {
			return fmt.Errorf("client.NewReq('%s', '%d'): %w", depConfig.Id, depConfig.Port, err)
		}
		c.DepClients.Set(depConfig.Id, depClient)
	}

	return nil
}

// Close the handler
func (c *Handler) Close() error {
	if c.InstanceManager.Status() == instance_manager.Running {
		c.InstanceManager.Close()
	}
	if c.Reactor.Status() == reactor.RUNNING {
		if err := c.Reactor.Close(); err != nil {
			return fmt.Errorf("c.Reactor.Close: %w", err)
		}
	}
	if c.Manager.Status() == handler_manager.SocketReady {
		c.Manager.Close()
	}

	return nil
}

// Runs the instance Manager and listen to it's events
func (c *Handler) RunInstanceManager() {
	socket, err := zmq.NewSocket(zmq.SUB)
	if err != nil {
		c.logger.Warn("zmq.NewSocket", "id", c.Config.Id, "function", "RunInstanceManager", "error", err)
		return
	}

	if err := socket.SetSubscribe(""); err != nil {
		c.logger.Warn("set subscriber", "id", c.Config.Id, "function", "RunInstanceManager", "error", err)
		return
	}

	url := config.InstanceManagerEventUrl(c.Config.Id)
	err = socket.Connect(url)
	if err != nil {
		c.logger.Warn("eventSocket.Connect", "id", c.Config.Id, "function", "RunInstanceManager", "url", url, "error", err)
		return
	}
	c.instanceManagerRuns = true

	go c.InstanceManager.Run()

	// The first Instance created by handler when the instance Manager is ready.
	firstInstance := false
	// Verify that the first instance was added.
	instanceId := ""

	for {
		raw, err := socket.RecvMessage(0)
		if err != nil {
			c.logger.Warn("eventSocket.RecvMessage", "id", c.Config.Id, "error", err)
			break
		}

		req, err := message.NewReq(raw)
		if err != nil {
			c.logger.Warn("eventSocket: convert raw to message", "id", c.Config.Id, "message", raw, "error", err)
			continue
		}

		if req.Command == instance_manager.EventReady {
			if !firstInstance {
				instanceId, err = c.InstanceManager.AddInstance(c.Config.Type, &c.Routes, &c.RouteDeps, &c.DepClients)
				if err != nil {
					c.logger.Warn("InstanceManager.AddInstance", "id", c.Config.Id, "event", req.Command, "type", c.Config.Type, "error", err)
					continue
				}
				firstInstance = true
			}
		} else if req.Command == instance_manager.EventInstanceAdded {
			if firstInstance && len(instanceId) > 0 {
				addedInstanceId, err := req.Parameters.GetString("id")
				if err != nil {
					c.logger.Warn("req.Parameters.GetString('id')", "id", c.Config.Id, "event", req.Command, "instanceId", instanceId, "error", err)
					continue
				}
				if addedInstanceId != instanceId {
					continue
				} else {
					c.logger.Info("first instance created added", "id", c.Config.Id, "event", req.Command, "instanceId", instanceId)
					instanceId = ""
				}
			}
		} else if req.Command == instance_manager.EventError {
			errMsg, err := req.Parameters.GetString("message")
			if err != nil {
				c.logger.Warn("req.Parameters.GetString('message')", "id", c.Config.Id, "event", req.Command, "error", err)
				continue
			}

			c.logger.Warn("instance Manager exited with an error", "id", c.Config.Id, "error", errMsg)
			break
		} else if req.Command == instance_manager.EventIdle {
			closeSignal, _ := req.Parameters.GetBoolean("close")
			c.logger.Warn("instance Manager is idle", "id", c.Config.Id, "close signal received?", closeSignal)
			if closeSignal {
				break
			}
		} else {
			c.logger.Warn("unhandled instance Manager event", "id", c.Config.Id, "event", req.Command, "parameters", req.Parameters)
		}
	}

	err = socket.Close()
	if err != nil {
		c.logger.Warn("failed to close instance Manager sub", "id", c.Config.Id, "error", err)
	}

	c.instanceManagerRuns = false
}

func (c *Handler) Status() string {
	return c.status
}

// Start the handler directly, not by goroutine
func (c *Handler) Start() error {
	if c.Config == nil {
		return fmt.Errorf("configuration not set")
	}
	if err := c.depConfigsAdded(); err != nil {
		return fmt.Errorf("depConfigsAdded: %w", err)
	}
	if c.InstanceManager == nil {
		return fmt.Errorf("instance Manager not set")
	}
	if c.Reactor == nil {
		return fmt.Errorf("Reactor not set")
	}
	if err := c.initDepClients(); err != nil {
		return fmt.Errorf("initDepClients: %w", err)
	}

	// Adding the first instance Manager
	go c.Reactor.Run()
	go c.RunInstanceManager()
	go func() {
		err := c.Manager.Run()
		if err != nil {
			c.status = err.Error()
		}
	}()

	return nil
}
