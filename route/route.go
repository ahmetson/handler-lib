package route

import (
	"github.com/ahmetson/client-lib"
)

// Any route name
const Any string = "*"

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
