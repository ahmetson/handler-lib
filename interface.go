package handler

import (
	"fmt"
	clientConfig "github.com/ahmetson/client-lib/config"
	"github.com/ahmetson/common-lib/data_type/key_value"
	"github.com/ahmetson/common-lib/message"
	"github.com/ahmetson/handler-lib/command"
	"github.com/ahmetson/handler-lib/config"
)

// Interface of the server.
// All controllers have
//
// The interface that it accepts is the *client.ClientSocket from the
// "github.com/ahmetson/client-lib" package.
//
// handler.New(handler.Type)
// handler.SetConfig(config)
// handler.Route("hello", onHello)
//
// The service will call:
// AddDepByService
type Interface interface {
	// SetConfig adds the parameters of the server from the config
	SetConfig(*config.Handler)

	// AddDepByService adds the config of the extension that the server depends on
	AddDepByService(*clientConfig.Client)

	// Deps return the list of dep ids collected from all routes.
	Deps() []string

	// Route registers a new command and it's handlers for this server
	Route(string, any, ...string) error

	// Type returns the type of the server
	Type() config.HandlerType

	// Close the server if it's running. If it's not running, then do nothing
	Close() error

	Run() error
}

// Does nothing, simply returns the data
var anyHandler = func(request message.Request) message.Reply {
	replyParameters := key_value.Empty()
	replyParameters.Set("command", request.Command)

	reply := request.Ok(replyParameters)
	return reply
}

// AnyRoute makes the given server as the source of the proxy.
// It means it will add command.Any to call the proxy.
func AnyRoute(sourceController Interface) error {
	if err := sourceController.Route(command.Any, anyHandler); err != nil {
		return fmt.Errorf("failed to add any route into the server: %w", err)
	}
	return nil
}

func requiredMetadata() []string {
	return []string{"Identity", "pub_key"}
}
