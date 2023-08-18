package route

import (
	"fmt"
	"github.com/ahmetson/client-lib"
	"github.com/ahmetson/common-lib/data_type/key_value"
	"github.com/ahmetson/common-lib/message"
)

// Route is the route, handler of the route
// and the extensions that this route depends on.
type Route struct {
	Extensions []string
	handler    interface{}
	variant    int
}

// Any route name
const Any string = "*"

// Routes Binding of Command to the Command Handler.
type Routes = key_value.List

// NewRoutes returns an empty routes
func NewRoutes() *Routes {
	return key_value.NewList()
}

// NewRoute returns a new route handler. It's used by the controllers.
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

// FilterExtensionClients returns the list of the clients specific for this route
func FilterExtensionClients(deps []string, clients client.Clients) []*client.ClientSocket {
	routeClients := make([]*client.ClientSocket, len(deps))

	added := 0
	for extensionName := range clients {
		for i := 0; i < len(deps); i++ {
			if deps[i] == extensionName {
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
