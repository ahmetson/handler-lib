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
