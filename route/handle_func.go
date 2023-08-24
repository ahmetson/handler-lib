package route

import (
	"fmt"
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

// DepAmount returns -1 if the interface is not a valid HandleFunc.
// If the interface has more than 3 arguments, it returns 4.
// Otherwise, it returns 0..3
func DepAmount(handleInterface interface{}) int {
	_, ok := handleInterface.(HandleFunc0)
	if ok {
		return 0
	}
	_, ok = handleInterface.(HandleFunc1)
	if ok {
		return 1
	}
	_, ok = handleInterface.(HandleFunc2)
	if ok {
		return 2
	}
	_, ok = handleInterface.(HandleFunc3)
	if ok {
		return 3
	}
	_, ok = handleInterface.(HandleFuncN)
	if ok {
		return 4
	}

	return -1
}

// Handle calls the handle func for the req.
// Optionally, if the handler requires the extensions, it will pass the socket clients to the handle func.
func Handle(req *message.Request, handleInterface interface{}, depClients []*client.ClientSocket) *message.Reply {
	var reply message.Reply

	depAmount := DepAmount(handleInterface)
	if depAmount == -1 {
		reply = req.Fail("handleInterface is not a HandleFunc")
		return &reply
	}
	if depAmount > 3 && len(depClients) < 4 {
		reply = req.Fail("handle func requires N deps, but Handle func received less dep clients")
		return &reply
	}
	if depAmount != len(depClients) {
		reply = req.Fail(fmt.Sprintf("handle func requires %d deps, but Handle received %d dep clients", depAmount, len(depClients)))
		return &reply
	}

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

// IsHandleFunc returns true if the given interface is convertible into HandleFunc
func IsHandleFunc(handleInterface interface{}) bool {
	_, ok := handleInterface.(HandleFunc0)
	if ok {
		return true
	}
	_, ok = handleInterface.(HandleFunc1)
	if ok {
		return true
	}

	_, ok = handleInterface.(HandleFunc2)
	if ok {
		return true
	}

	_, ok = handleInterface.(HandleFunc3)
	if ok {
		return true
	}

	_, ok = handleInterface.(HandleFuncN)
	if ok {
		return true
	}

	return false
}
