package command

import (
	"fmt"
	"github.com/ahmetson/client-lib"
	"github.com/ahmetson/common-lib/data_type/key_value"
	"github.com/ahmetson/common-lib/message"
)

type depSock = *client.ClientSocket

// HandleFunc0 is the function type that manipulates the commands.
// It accepts at least message.Request and log.Logger then returns message.Reply.
//
// Optionally, the server can pass the shared states in the additional parameters.
// The most use case for optional request is to pass the link to the Database.
type HandleFunc0 = func(message.Request) message.Reply
type HandleFunc1 = func(message.Request, depSock) message.Reply
type HandleFunc2 = func(message.Request, depSock, depSock) message.Reply
type HandleFunc3 = func(message.Request, depSock, depSock, depSock) message.Reply
type HandleFuncN = func(message.Request, ...depSock) message.Reply

// Route is the command, handler of the command
// and the extensions that this command depends on.
type Route struct {
	Extensions []string
	handler    interface{}
	variant    int
}

// Any command name
const Any string = "*"

// Routes Binding of Command to the Command Handler.
type Routes = key_value.List

// NewRoutes returns an empty routes
func NewRoutes() *Routes {
	return key_value.NewList()
}

// NewRoute returns a new command handler. It's used by the controllers.
func NewRoute(handler interface{}, extensions ...string) *Route {
	return &Route{
		Extensions: extensions,
		handler:    handler,
	}
}

// AddHandler if the handler already exists, then it will throw an error
func (route *Route) AddHandler(handler interface{}) error {
	if route.handler == nil {
		route.handler = handler
		return nil
	}

	return fmt.Errorf("handler exists in route")
}

// FilterExtensionClients returns the list of the clients specific for this command
func (route *Route) filterExtensionClients(clients client.Clients) []*client.ClientSocket {
	routeClients := make([]*client.ClientSocket, len(route.Extensions))

	added := 0
	for extensionName := range clients {
		for i := 0; i < len(route.Extensions); i++ {
			if route.Extensions[i] == extensionName {
				routeClients[added] = clients[extensionName].(*client.ClientSocket)
				added++
			}
		}
	}

	return routeClients
}

//func (route *Route) Handle(request message.Request, logger *log.Logger, allExtensions client.Clients) message.Reply {
//	extensions := route.filterExtensionClients(allExtensions)
//	return route.handler(request, logger, extensions...)
//}

// Reply creates a successful message.Reply with the given reply parameters.
func Reply(reply interface{}) (message.Reply, error) {
	replyParameters, err := key_value.NewFromInterface(reply)
	if err != nil {
		return message.Reply{}, fmt.Errorf("failed to encode reply: %w", err)
	}

	return message.Reply{
		Status:     message.OK,
		Message:    "",
		Parameters: replyParameters,
	}, nil
}

// Commands returns the commands from the routes
func Commands(routes *Routes) []string {
	commands := make([]string, routes.Len())

	list := routes.List()

	i := 0
	for name := range list {
		commands[i] = name.(string)
		i++
	}

	return commands
}
