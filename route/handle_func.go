package route

import (
	"github.com/ahmetson/client-lib"
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

// Handle calls the handle func for the req.
// Optionally, if the handler requires the extensions, it will pass the socket clients to the handle func.
func Handle(req *message.Request, handleInterface interface{}, depClients []*client.ClientSocket) *message.Reply {
	var reply message.Reply

	if len(depClients) == 0 {
		handleFunc := handleInterface.(HandleFunc0)
		reply = handleFunc(*req)
	} else if len(depClients) == 1 {
		handleFunc := handleInterface.(HandleFunc1)
		reply = handleFunc(*req, depClients[0])
	} else if len(depClients) == 2 {
		handleFunc := handleInterface.(HandleFunc2)
		reply = handleFunc(*req, depClients[0], depClients[1])
	} else if len(depClients) == 3 {
		handleFunc := handleInterface.(HandleFunc3)
		reply = handleFunc(*req, depClients[0], depClients[1], depClients[2])
	} else {
		handleFunc := handleInterface.(HandleFuncN)
		reply = handleFunc(*req, depClients...)
	}

	return &reply
}
