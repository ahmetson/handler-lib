// Package base keeps the generic Handler.
// It's not intended to be used independently.
// Other handlers should be defined based on this handler
package base

import (
	"fmt"
	"github.com/ahmetson/client-lib"
	service "github.com/ahmetson/client-lib/config"
	"github.com/ahmetson/common-lib/data_type/key_value"
	"github.com/ahmetson/handler-lib/config"
	"github.com/ahmetson/handler-lib/route"
	"github.com/ahmetson/log-lib"
	"github.com/ahmetson/os-lib/net"
	"github.com/ahmetson/os-lib/process"
	"slices"

	"github.com/ahmetson/common-lib/message"
	zmq "github.com/pebbe/zmq4"
)

// The Handler is the socket wrapper for the service.
type Handler struct {
	config     *config.Handler
	socket     *zmq.Socket
	logger     *log.Logger
	routes     key_value.KeyValue
	routeDeps  key_value.KeyValue
	depIds     []string
	depConfigs key_value.KeyValue
	depClients client.Clients
}

// New handler
func New() *Handler {
	return &Handler{
		logger:     nil,
		routes:     key_value.Empty(),
		routeDeps:  key_value.Empty(),
		depIds:     make([]string, 0),
		depConfigs: key_value.Empty(),
		depClients: key_value.Empty(),
	}
}

// SetConfig adds the parameters of the server from the config.
func (c *Handler) SetConfig(controller *config.Handler) {
	c.config = controller
}

// SetLogger sets the logger.
func (c *Handler) SetLogger(parent *log.Logger) error {
	if c.config == nil {
		return fmt.Errorf("missing configuration")
	}
	logger := parent.Child(c.config.Id)
	c.logger = logger

	return nil
}

// AddDepByService adds the config of the dependency. Intended to be called by Service not by developer
func (c *Handler) AddDepByService(dep *service.Client) {
	c.depConfigs.Set(dep.Id, dep)
}

// addDep marks the depClients that this server depends on.
// Before running, the required extension should be added from the config.
// Otherwise, server won't run.
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

// extensionsAdded checks that the required depClients are added into the server.
// If no depClients are added by calling server.addDep(), then it will return nil.
func (c *Handler) extensionsAdded() error {
	for _, id := range c.depIds {
		if err := c.depConfigs.Exist(id); err != nil {
			return fmt.Errorf("'%s' dependency not added", id)
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

// initExtensionClients will set up the extension clients for this server.
// It will be called by c.Run(), automatically.
//
// The reason for why we call it by c.Run() is due to the thread-safety.
//
// The server is intended to be called as the goroutine. And if the sockets
// are not initiated within the same goroutine, then zeromq doesn't guarantee the socket work
// as it's intended.
func (c *Handler) initExtensionClients() error {
	for _, extensionInterface := range c.depConfigs {
		extensionConfig := extensionInterface.(*service.Client)
		extension, err := client.NewReq(extensionConfig.Url, extensionConfig.Port, c.logger)
		if err != nil {
			return fmt.Errorf("failed to create a request client: %w", err)
		}
		c.depClients.Set(extensionConfig.Url, extension)
	}

	return nil
}

func (c *Handler) Close() error {
	if c.socket == nil {
		return nil
	}

	err := c.socket.Close()
	if err != nil {
		return fmt.Errorf("server.socket.Close: %w", err)
	}

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