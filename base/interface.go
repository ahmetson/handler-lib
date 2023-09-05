package base

import (
	"fmt"
	clientConfig "github.com/ahmetson/client-lib/config"
	"github.com/ahmetson/common-lib/data_type/key_value"
	"github.com/ahmetson/common-lib/message"
	"github.com/ahmetson/handler-lib/config"
	"github.com/ahmetson/handler-lib/route"
	"github.com/ahmetson/log-lib"
)

// Interface of the handler. Any handlers must be based on this.
// All handlers have
//
// The interface that it accepts is the *client.ClientSocket from the
// "github.com/ahmetson/client-lib" package.
//
// handler.New(handler.Type)
// handler.SetConfig(Config)
// handler.Route("hello", onHello)
//
// The service will call:
// AddDepByService
type Interface interface {
	Config() *config.Handler
	// SetConfig adds the parameters of the handler from the Config
	SetConfig(*config.Handler)

	// SetLogger adds the logger. The function accepts a parent, and function derives handler logger
	// Requires configuration to be set first
	SetLogger(*log.Logger) error

	// AddDepByService adds the Config of the extension that the handler depends on.
	// This function is intended to be called by the service.
	//
	// If any route does not require the dependency, it returns an error.
	// If the configuration already added, it returns an error.
	AddDepByService(*clientConfig.Client) error

	// AddedDepByService returns true if the configuration already exists
	AddedDepByService(string) bool

	// DepIds return the list of dep ids collected from all Routes.
	DepIds() []string

	// Route adds a new route and it's handlers for this handler
	Route(string, any, ...string) error

	// Type returns the type of the handler
	Type() config.HandlerType

	// Close the handler if it's running. If it's not running, then do nothing
	Close() error

	Start() error

	// The Status is empty is the handler is running.
	// Returns an error string if the Manager is not running
	Status() string
}

// Does nothing, simply returns the data
var anyHandler = func(request message.Request) message.Reply {
	replyParameters := key_value.Empty()
	replyParameters.Set("route", request.Command)

	reply := request.Ok(replyParameters)
	return reply
}

func AnyRoute(handler Interface) error {
	if err := handler.Route(route.Any, anyHandler); err != nil {
		return fmt.Errorf("failed to '%s' route into the handler: %w", route.Any, err)
	}
	return nil
}

func requiredMetadata() []string {
	return []string{"Identity", "pub_key"}
}
