package handler

import (
	"fmt"
	"github.com/ahmetson/client-lib"
	clientConfig "github.com/ahmetson/client-lib/config"
	"github.com/ahmetson/common-lib/data_type/key_value"
	"github.com/ahmetson/common-lib/message"
	"github.com/ahmetson/handler-lib/command"
	"github.com/ahmetson/handler-lib/config"
	"github.com/ahmetson/log-lib"
)

// Interface of the server.
// All controllers have
//
// The interface that it accepts is the *client.ClientSocket from the
// "github.com/ahmetson/client-lib" package.
type Interface interface {
	// AddConfig adds the parameters of the server from the config
	AddConfig(controller *config.Handler, serviceUrl string)

	// AddExtensionConfig adds the config of the extension that the server depends on
	AddExtensionConfig(extension *clientConfig.Client)

	// RequireExtension marks the extensions that this server depends on.
	// Before running, the required extension should be added from the config.
	// Otherwise, server won't run.
	RequireExtension(name string)

	// RequiredExtensions returns the list of extension names required by this server
	RequiredExtensions() []string

	// AddRoute registers a new command and it's handlers for this server
	AddRoute(route *command.Route) error

	// ControllerType returns the type of the server
	ControllerType() config.HandlerType

	// Close the server if it's running. If it's not running, then do nothing
	Close() error

	Run() error
}

// Does nothing, simply returns the data
var anyHandler = func(request message.Request, _ *log.Logger, _ ...*client.ClientSocket) message.Reply {
	replyParameters := key_value.Empty()
	replyParameters.Set("command", request.Command)

	reply := request.Ok(replyParameters)
	return reply
}

// AnyRoute makes the given server as the source of the proxy.
// It means it will add command.Any to call the proxy.
func AnyRoute(sourceController Interface) error {
	route := command.NewRoute(command.Any, anyHandler)

	if err := sourceController.AddRoute(route); err != nil {
		return fmt.Errorf("failed to add any route into the server: %w", err)
	}
	return nil
}

func requiredMetadata() []string {
	return []string{"Identity", "pub_key"}
}
