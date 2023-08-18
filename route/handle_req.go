package route

import (
	"fmt"
	"github.com/ahmetson/client-lib"
	"github.com/ahmetson/common-lib/data_type/key_value"
	"github.com/ahmetson/common-lib/message"
)

//
// Handle Requests to redirect to a certain handle function
//

// HandleFunc finds the dependencies and the handling function for the given command.
//
// Note that in golang, returning interfaces are considered a bad practice.
// However, we do still return an interface{} as this interface will be a different type of HandleFunc.
func HandleFunc(cmd string, routeFuncs key_value.KeyValue, routeDeps key_value.KeyValue) (interface{}, []string, error) {
	var handleInterface interface{}
	var err error
	var handleDeps []string

	if err := routeFuncs.Exist(cmd); err == nil {
		handleInterface = routeFuncs[cmd]
		if err := routeDeps.Exist(cmd); err == nil {
			handleDeps, err = routeDeps.GetStringList(cmd)
		}
	} else if err := routeFuncs.Exist(Any); err == nil {
		handleInterface = routeFuncs[Any]
		if err := routeDeps.Exist(Any); err == nil {
			handleDeps, err = routeDeps.GetStringList(Any)
		}
	} else {
		err = fmt.Errorf("handler not found for route: %s", cmd)
	}

	return handleInterface, handleDeps, err
}

// HandleReq calls the handle func for the req.
// Optionally, if the handler requires the extensions, it will pass the socket clients to the handle func.
func HandleReq(req *message.Request, handleInterface interface{}, handleDeps []string, clients key_value.KeyValue) *message.Reply {
	var reply message.Reply

	if len(handleDeps) == 0 {
		handleFunc := handleInterface.(HandleFunc0)
		reply = handleFunc(*req)
	} else if len(handleDeps) == 1 {
		handleFunc := handleInterface.(HandleFunc1)
		ext1 := clients[handleDeps[0]].(*client.ClientSocket)
		reply = handleFunc(*req, ext1)
	} else if len(handleDeps) == 2 {
		handleFunc := handleInterface.(HandleFunc2)
		ext1 := clients[handleDeps[0]].(*client.ClientSocket)
		ext2 := clients[handleDeps[1]].(*client.ClientSocket)
		reply = handleFunc(*req, ext1, ext2)
	} else if len(handleDeps) == 3 {
		handleFunc := handleInterface.(HandleFunc3)
		ext1 := clients[handleDeps[0]].(*client.ClientSocket)
		ext2 := clients[handleDeps[1]].(*client.ClientSocket)
		ext3 := clients[handleDeps[3]].(*client.ClientSocket)
		reply = handleFunc(*req, ext1, ext2, ext3)
	} else {
		handleFunc := handleInterface.(HandleFuncN)
		depClients := FilterExtensionClients(handleDeps, clients)
		reply = handleFunc(*req, depClients...)
	}

	return &reply
}
