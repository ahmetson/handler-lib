package handler

import (
	"fmt"
	"github.com/ahmetson/client-lib"
	service "github.com/ahmetson/client-lib/config"
	"github.com/ahmetson/common-lib/data_type/key_value"
	"github.com/ahmetson/handler-lib/config"
	"github.com/ahmetson/log-lib"

	"github.com/ahmetson/common-lib/message"
	"github.com/ahmetson/handler-lib/command"
	zmq "github.com/pebbe/zmq4"
)

// The Handler is the socket wrapper for the service.
type Handler struct {
	config             *config.Handler
	socket             *zmq.Socket
	logger             *log.Logger
	controllerType     config.HandlerType
	routes             *command.Routes
	requiredExtensions []string
	extensionConfigs   key_value.KeyValue
	extensions         client.Clients
}

// AddConfig adds the parameters of the server from the config.
func (c *Handler) AddConfig(controller *config.Handler) {
	c.config = controller
}

// AddExtensionConfig adds the config of the extension that the server depends on
func (c *Handler) AddExtensionConfig(extension *service.Client) {
	c.extensionConfigs.Set(extension.Url, extension)
}

// RequireExtension marks the extensions that this server depends on.
// Before running, the required extension should be added from the config.
// Otherwise, server won't run.
func (c *Handler) RequireExtension(name string) {
	c.requiredExtensions = append(c.requiredExtensions, name)
}

// RequiredExtensions returns the list of extension names required by this server
func (c *Handler) RequiredExtensions() []string {
	return c.requiredExtensions
}

func (c *Handler) isReply() bool {
	return c.controllerType == config.SyncReplierType
}

// A reply sends to the caller the message.
//
// If a server doesn't support replying (for example, PULL server),
// then it returns success.
func (c *Handler) reply(socket *zmq.Socket, message message.Reply) error {
	if !c.isReply() {
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

// AddRoute adds a command along with its handler to this server
func (c *Handler) AddRoute(route *command.Route) error {
	if c.routes.Exist(route.Command) {
		return nil
	}

	err := c.routes.Add(route.Command, route)
	if err != nil {
		return fmt.Errorf("failed to add a route: %w", err)
	}

	return nil
}

// extensionsAdded checks that the required extensions are added into the server.
// If no extensions are added by calling server.RequireExtension(), then it will return nil.
func (c *Handler) extensionsAdded() error {
	for _, name := range c.requiredExtensions {
		if err := c.extensionConfigs.Exist(name); err != nil {
			return fmt.Errorf("required '%s' extension. but it wasn't added to the server (does it exist in config.yml?)", name)
		}
	}

	return nil
}

func (c *Handler) ControllerType() config.HandlerType {
	return c.controllerType
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
	for _, extensionInterface := range c.extensionConfigs {
		extensionConfig := extensionInterface.(*service.Client)
		extension, err := client.NewReq(extensionConfig.Url, extensionConfig.Port, c.logger)
		if err != nil {
			return fmt.Errorf("failed to create a request client: %w", err)
		}
		c.extensions.Set(extensionConfig.Url, extension)
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

// Url creates url of the server url for binding.
// For clients to connect to this url, call client.ClientUrl()
func Url(name string, port uint64) string {
	if port == 0 {
		return fmt.Sprintf("inproc://%s", name)
	}
	url := fmt.Sprintf("tcp://*:%d", port)
	return url
}
