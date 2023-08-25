// Package reactor forwards incoming messages to the instances and vice versa.
package reactor

import (
	"fmt"
	"github.com/ahmetson/common-lib/data_type"
	"github.com/ahmetson/common-lib/data_type/key_value"
	"github.com/ahmetson/common-lib/message"
	"github.com/ahmetson/handler-lib/config"
	"github.com/ahmetson/handler-lib/instance_manager"
	zmq "github.com/pebbe/zmq4"
	"time"
)

const (
	CREATED = "created"
	RUNNING = "running"
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

func (reactor *Reactor) QueueLen() uint {
	return reactor.queue.Len()
}

func (reactor *Reactor) ProcessingLen() uint {
	return reactor.processing.Len()
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

	// Keep in memory one message
	// Rather than queueing the messages in the Zeromq, we queue in Reactor
	if err := external.SetRcvhwm(1); err != nil {
		return fmt.Errorf("external.SetRcvhwm(1): %w", err)
	}

	url := config.ExternalUrl(reactor.externalConfig.Id, reactor.externalConfig.Port)
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

// Run the reactor
func (reactor *Reactor) Run() {
	if reactor.externalConfig == nil {
		reactor.status = fmt.Sprintf("missing externalConfig")
		return
	}

	// Instance Manager maybe not running. We won't block the reactor from it.
	if reactor.instanceManager == nil {
		reactor.status = fmt.Sprintf("missing instanceManager")
		return
	}

	if err := reactor.prepareExternalSocket(); err != nil {
		reactor.status = fmt.Sprintf("prepareExternalSocket: %v", err)
		return
	}

	reactor.prepareSockets()
	reactor.receiveExternalMessages()
	reactor.runConsumer()

	reactor.status = RUNNING
	for {
		if reactor.close {
			break
		}

		// Zeromq's reactor (sockets) sets the poll timeout not as infinite, for two reasons.
		// First, zeromq's reactor requires a timeout if we add a channel.
		// The Reactor's consumer is channel-based.
		// The second reason is due to external socket.
		// If the external socket receives a message, but the queue is full, then the message will be returned back
		if err := reactor.sockets.Run(time.Millisecond); err != nil {
			if !reactor.close {
				reactor.status = fmt.Sprintf("sockets.Run (mark as closed? %v): %v", reactor.close, err)
				return
			}
			break // break if marked as close
		}
	}

	// exited due to closing? reset it
	if reactor.close {
		reactor.close = false
		reactor.status = CREATED
	}
}

// handleFrontend is an event invoked by the zmq4.Reactor whenever a new client request happens.
//
// This function will forward the messages to the backend.
// Since backend is calling the workers, which means the worker will be busy, this function removes the worker from the queue.
// Since the queue is removed, it will remove the external from the reactor.
// Frontend will still receive the messages, however, they will be queued until external will not be added to the reactor.
func (reactor *Reactor) handleFrontend() error {
	msg, err := reactor.external.RecvMessage(0)
	if err != nil {
		return fmt.Errorf("handleFrontend: external.RecvMessage: %w", err)
	}
	req, err := message.NewReq(msg[2:])
	if err != nil {
		return fmt.Errorf("handleFrontend: message.NewReq: %w", err)
	}

	if reactor.queue.IsFull() {
		reply := req.Fail("queue is full")
		replyStr, err := reply.String()
		if err != nil {
			return fmt.Errorf("handleFrontend: reply.String: %w", err)
		}
		if _, err := reactor.external.SendMessageDontwait(msg[0], msg[1], replyStr); err != nil {
			return fmt.Errorf("handleFrontend: reactor.external.SendMessageDontwait: %w", err)
		}
		return nil
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
	if reactor.close {
		return nil
	}

	if reactor.status != RUNNING {
		return nil
	}

	reactor.close = true // stop zeromq's reactor running

	if reactor.sockets == nil {
		return nil
	}

	if err := reactor.closeExternal(); err != nil {
		return fmt.Errorf("reactor.closeExternal: %w", err)
	}

	// remove consumer from zeromq's reactor
	if reactor.consumerId > 0 {
		reactor.sockets.RemoveChannel(reactor.consumerId)
		reactor.consumerId = 0
	}

	// remove instance sockets from zeromq's reactor
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
	// Maybe it's still loading
	if reactor.instanceManager.Status() != instance_manager.Running {
		return nil
	}

	if reactor.sockets == nil {
		return fmt.Errorf("zmq.Reactor not set")
	}

	if reactor.queue.IsEmpty() {
		return nil
	}

	id, sock := reactor.instanceManager.Ready()
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
