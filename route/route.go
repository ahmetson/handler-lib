package route

import (
	"fmt"
	"github.com/ahmetson/client-lib"
	"github.com/ahmetson/common-lib/data_type/key_value"
)

// Any route name
const Any string = "*"

// FilterExtensionClients returns the list of the clients specific for this route
func FilterExtensionClients(deps []string, clients client.Clients) []*client.ClientSocket {
	routeClients := make([]*client.ClientSocket, len(deps))

	for i := 0; i < len(deps); i++ {
		found := false

		for extensionName := range clients {
			if deps[i] == extensionName {
				routeClients[i] = clients[extensionName].(*client.ClientSocket)
				found = true
				break
			}
		}

		if !found {
			routeClients[i] = nil
		}
	}

	return routeClients
}

// Route finds the dependencies and the handling function for the given command.
//
// Note that in golang, returning interfaces are considered a bad practice.
// However, we do still return an interface{} as this interface will be a different type of Route.
//
// Returns handle func from the func list, returns dependencies list and error
func Route(cmd string, routeFuncs key_value.KeyValue, routeDeps key_value.KeyValue) (interface{}, []string, error) {
	var handleInterface interface{}
	var handleDeps []string
	var err error

	err = routeFuncs.Exist(cmd)
	if err == nil {
		handleInterface = routeFuncs[cmd]
		err = routeDeps.Exist(cmd)
		fmt.Printf("checkins is %s has deps: %v\n", cmd, err)
		if err == nil {
			handleDeps, err = routeDeps.GetStringList(cmd)
		} else {
			err = nil
		}
	} else {
		err = routeFuncs.Exist(Any)
		if err == nil {
			handleInterface = routeFuncs[Any]
			err = routeDeps.Exist(Any)
			if err == nil {
				handleDeps, err = routeDeps.GetStringList(Any)
			} else {
				err = nil
			}
		}
	}

	return handleInterface, handleDeps, err
}
