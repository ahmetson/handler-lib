// Package reactor forwards incoming messages to the instances and vice versa.
package reactor

import (
	"fmt"
	"github.com/ahmetson/common-lib/data_type"
	"github.com/ahmetson/handler-lib/config"
	zmq "github.com/pebbe/zmq4"
)

type Reactor struct {
	external       *zmq.Socket
	redirect       *zmq.Socket
	sockets        *zmq.Reactor
	id             string // Handler ID
	status         string
	externalConfig *config.Handler
	queue          *data_type.Queue
	queueCap       uint
}

// New reactor is created
func New() *Reactor {
	return &Reactor{
		external:       nil,
		redirect:       nil,
		status:         CREATED,
		externalConfig: nil,
		queue:          data_type.NewQueue(),
		queueCap:       100_000,
	}
}

func (reactor *Reactor) SetConfig(externalConfig *config.Handler) {
	reactor.externalConfig = externalConfig
}

// prepareExternalSocket sets up the external socket that bind to the url from externalConfig.
func (reactor *Reactor) prepareExternalSocket() error {
	socketType := config.SocketType(reactor.externalConfig.Type)
	if reactor.externalConfig.Type == config.SyncReplierType {
		socketType = config.SocketType(config.ReplierType)
	}

	external, err := zmq.NewSocket(socketType)
	if err != nil {
		return fmt.Errorf("newFrontend(%s): %v", reactor.externalConfig.Type, err)
	}

	url := config.HandleUrl(reactor.externalConfig.Id, reactor.externalConfig.Port)
	err = external.Bind(url)
	if err != nil {
		return fmt.Errorf("external(%s).Bind(%s): %v", reactor.externalConfig.Type, url, err)
	}
	reactor.external = external

	return nil
}

// prepareSockets sets up the zeromq's reactor to handle all sockets
func (reactor *Reactor) prepareSockets() {
	reactor.sockets = zmq.NewReactor()
}

func (reactor *Reactor) runExternalReceiver() {
	reactor.sockets.AddSocket(reactor.external, zmq.POLLIN, func(e zmq.State) error { return reactor.handleFrontend() })
}

// handleFrontend is an event invoked by the zmq4.Reactor whenever a new client request happens.
//
// This function will forward the messages to the backend.
// Since backend is calling the workers, which means the worker will be busy, this function removes the worker from the queue.
// Since the queue is removed, it will remove the external from the reactor.
// Frontend will still receive the messages, however, they will be queued until external will not be added to the reactor.
func (reactor *Reactor) handleFrontend() error {
	if reactor.queue.IsFull() {
		return fmt.Errorf("queue full")
	}

	msg, err := reactor.external.RecvMessage(0)
	fmt.Printf("recevied: '%s', '%v'\n", msg[2:], err)
	if err != nil {
		return fmt.Errorf("external.RecvMessage: %w", err)
	}

	reactor.queue.Push(msg)

	return nil
}
