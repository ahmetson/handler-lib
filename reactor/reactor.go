// Package reactor forwards incoming messages to the instances and vice versa.
package reactor

import (
	"fmt"
	"github.com/ahmetson/common-lib/data_type"
	"github.com/ahmetson/common-lib/data_type/key_value"
	"github.com/ahmetson/handler-lib/config"
	"github.com/ahmetson/handler-lib/instance_manager"
	zmq "github.com/pebbe/zmq4"
	"time"
)

const (
	CREATED = "created"
)

type Reactor struct {
	external        *zmq.Socket
	sockets         *zmq.Reactor
	id              string // Handler ID
	status          string
	externalConfig  *config.Handler
	queue           *data_type.Queue
	processing      *key_value.List // Tracking messages processed by the instances
	instanceManager *instance_manager.Parent
	consumerId      uint64
}

// New reactor is created
func New() *Reactor {
	return &Reactor{
		external:        nil,
		status:          CREATED,
		externalConfig:  nil,
		queue:           data_type.NewQueue(),
		processing:      key_value.NewList(),
		instanceManager: nil,
	}
}

func (reactor *Reactor) SetConfig(externalConfig *config.Handler) {
	reactor.externalConfig = externalConfig
}

func (reactor *Reactor) SetInstanceManager(manager *instance_manager.Parent) {
	reactor.instanceManager = manager
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

func (reactor *Reactor) runConsumer() {
	reactor.consumerId = reactor.sockets.AddChannelTime(time.Tick(time.Millisecond), 0,
		func(_ interface{}) error { return reactor.handleConsume() })
}

func (reactor *Reactor) runInstanceReceiver(id string, socket *zmq.Socket) {
	reactor.sockets.AddSocket(socket, zmq.POLLIN, func(e zmq.State) error { return reactor.handleInstance(id, socket) })
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

// Handling incoming messages
func (reactor *Reactor) handleInstance(id string, sock *zmq.Socket) error {
	messages, err := sock.RecvMessage(0)
	if err != nil {
		return fmt.Errorf("instance(%s).socket.ReceiveMessage: %w", id, err)
	}

	messageIdRaw, err := reactor.processing.Get(id)
	if err != nil {
		return fmt.Errorf("processing.Get(%s): %w", id, err)
	}

	messageId := messageIdRaw.(string)

	if _, err := reactor.external.SendMessageDontwait(messageId, "", messages); err != nil {
		return fmt.Errorf("external.SendMessageDontWaint(%v): %w", []byte(messageId), err)
	}

	// Delete the instance from the list
	_, err = reactor.processing.Take(id)
	if err != nil {
		return fmt.Errorf("processing.Take(%s): %w", id, err)
	}

	return nil
}

// This one consumes the message queue by each instance
func (reactor *Reactor) handleConsume() error {
	if reactor.instanceManager == nil {
		return fmt.Errorf("instanceManager not set")
	}

	if reactor.sockets == nil {
		return fmt.Errorf("zmq.Reactor not set")
	}

	if reactor.queue.IsEmpty() {
		return nil
	}

	id, sock := reactor.instanceManager.Ready()
	fmt.Printf("instanceManager %s, has sock? %v\n", reactor.instanceManager.Status(), sock)
	if reactor.processing.Exist(id) {
		return nil
	}

	if sock == nil {
		return nil
	}

	messages := reactor.queue.Pop().([]string)
	if err := reactor.processing.Add(id, messages[0]); err != nil {
		return fmt.Errorf("processing.Add: %w", err)
	}

	if _, err := sock.SendMessageDontwait(messages[2:]); err != nil {
		return fmt.Errorf("instance.SendMessageDontWait: %w", err)
	}

	reactor.runInstanceReceiver(id, sock)

	return nil
}