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
	close           bool
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
		consumerId:      0,
		close:           false,
	}
}

func (reactor *Reactor) SetConfig(externalConfig *config.Handler) {
	reactor.externalConfig = externalConfig
}

func (reactor *Reactor) SetInstanceManager(manager *instance_manager.Parent) {
	reactor.instanceManager = manager
}

func (reactor *Reactor) Status() string {
	return reactor.status
}

// IsError returns true if the reactor got issue during the running.
func (reactor *Reactor) IsError() bool {
	return reactor.status != CREATED && reactor.status != RUNNING
}

// prepareExternalSocket sets up the external socket.
// the user requests are coming to external socket.
// this socket is bound to the url from externalConfig.
func (reactor *Reactor) prepareExternalSocket() error {
	if err := reactor.closeExternal(); err != nil {
		return fmt.Errorf("reactor.closeExternal: %w", err)
	}

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

// prepareSockets sets up the zeromq's reactor to handle all External, instances in a one thread
func (reactor *Reactor) prepareSockets() {
	reactor.sockets = zmq.NewReactor()
}

// receiveExternalMessages adds the external socket to zeromq reactor to invoke handleFrontend when it receives a message.
func (reactor *Reactor) receiveExternalMessages() {
	reactor.sockets.AddSocket(reactor.external, zmq.POLLIN, func(e zmq.State) error { return reactor.handleFrontend() })
}

// runConsumer adds the consumer to the zeromq reactor to check for queue and send the messages to instances.
// It runs every millisecond, or 1000 times in a second.
func (reactor *Reactor) runConsumer() {
	reactor.consumerId = reactor.sockets.AddChannelTime(time.Tick(time.Millisecond), 0,
		func(_ interface{}) error { return reactor.handleConsume() })
}

// receiveInstanceMessage makes sure to invoke handleInstance when an instance returns the result.
func (reactor *Reactor) receiveInstanceMessage(id string, socket *zmq.Socket) {
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

func (reactor *Reactor) closeExternal() error {
	if reactor.external != nil {
		reactor.sockets.RemoveSocket(reactor.external)
		if err := reactor.external.Close(); err != nil {
			return fmt.Errorf("closing already running external socket: %w", err)
		}

		reactor.external = nil
	}

	return nil
}

// Close stops all sockets, and clear out the queue, processing
func (reactor *Reactor) Close() error {
	fmt.Printf("reactor closed? %v \n", reactor.close)
	if reactor.close {
		return nil
	}

	fmt.Printf("reactor status: %s, it's running? %v \n", reactor.status, reactor.status == RUNNING)
	if reactor.status != RUNNING {
		return nil
	}

	fmt.Printf("mark reactor as closed\n")
	reactor.close = true // stop zeromq's reactor running

	fmt.Printf("reactor sockets (zeromq's reactor) initialized? %v \n", reactor.sockets != nil)
	if reactor.sockets == nil {
		return nil
	}

	fmt.Printf("reactor closes external socket, has external socket? %v\n", reactor.external != nil)
	if err := reactor.closeExternal(); err != nil {
		return fmt.Errorf("reactor.closeExternal: %w", err)
	}
	fmt.Printf("reactor closes external socket, external socket closed? %v\n", reactor.external == nil)

	fmt.Printf("reactor closes consumer, has consumer? %v\n", reactor.consumerId > 0)

	// remove consumer from zeromq's reactor
	if reactor.consumerId > 0 {
		reactor.sockets.RemoveChannel(reactor.consumerId)
		reactor.consumerId = 0
	}
	fmt.Printf("reactor closes consumer, consumer closed? %v\n", reactor.consumerId == 0)

	// remove instance sockets from zeromq's reactor
	fmt.Printf("reactor closes instance receivers? %v, has instance manager? %v\n", reactor.processing.IsEmpty(), reactor.instanceManager != nil)
	if !reactor.processing.IsEmpty() && reactor.instanceManager != nil {
		list := reactor.processing.List()
		for raw := range list {
			instanceId := raw.(string)
			sock := reactor.instanceManager.Handler(instanceId)
			if sock != nil {
				reactor.sockets.RemoveSocket(sock)
			}
		}

		reactor.processing = key_value.NewList()
	}

	return nil
}

// Invoked when instances return a response.
// The invoked response returned to back to the external socket.
// The external socket will reply it back to the user.
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

	// avoiding receiving the incoming messages from instances again
	reactor.sockets.RemoveSocket(sock)

	return nil
}

// handleConsume forwards the messages from queue to the ready instances.
// requires instance manager and zeromq sockets to be set first.
//
// the forwards messages are taken from the queue and added to the processing list.
// it also registers instance in the zeromq reactor, so that Reactor could handle when the instance
// finishes its handling.
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

	reactor.receiveInstanceMessage(id, sock)

	return nil
}
