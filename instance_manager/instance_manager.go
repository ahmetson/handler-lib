// Package instance_manager manages the instances
package instance_manager

import (
	"fmt"
	"github.com/ahmetson/common-lib/data_type/key_value"
	"github.com/ahmetson/common-lib/message"
	"github.com/ahmetson/handler-lib/config"
	"github.com/ahmetson/handler-lib/instance"
	"github.com/ahmetson/log-lib"
	zmq "github.com/pebbe/zmq4"
	"time"
)

type kvRef = *key_value.KeyValue

const (
	InstanceCreated = "created" // instance is created, but not initialized yet
	Running         = "running"
	Idle            = "idle"
)

type Child struct {
	status        string      // instance status
	managerSocket *zmq.Socket // interact with the instance
}

type Parent struct {
	instances      map[string]*Child
	lastInstanceId uint
	socket         *zmq.Socket
	id             string
	logger         *log.Logger
	status         string
	close          bool
}

// New instance manager is created
func New(id string, parent *log.Logger) *Parent {
	logger := parent.Child("instance-manager")

	return &Parent{
		id:             id,
		lastInstanceId: 0,
		instances:      make(map[string]*Child, 0),
		socket:         nil,
		logger:         logger,
		status:         Idle,
		close:          false,
	}
}

// Status of the instance manager.
func (parent *Parent) Status() string {
	return parent.status
}

// onInstanceStatus updates the instance status.
// since our socket is one directional, there is no point to reply.
func (parent *Parent) onInstanceStatus(req message.Request) message.Reply {
	instanceId, err := req.Parameters.GetString("id")
	if err != nil {
		return req.Fail(fmt.Sprintf("req.Parameters.GetString('id'): %v", err))
	}

	status, err := req.Parameters.GetString("status")
	if err != nil {
		return req.Fail(fmt.Sprintf("req.Parameters.GetString('status'): %v", err))
	}

	child, ok := parent.instances[instanceId]
	if !ok {
		return req.Fail(fmt.Sprintf("instances[%s] not found", instanceId))
	}

	if child == nil {
		return req.Fail(fmt.Sprintf("instances[%s] is null", instanceId))
	}

	if status == instance.CLOSED {
		err = child.managerSocket.Close()
		delete(parent.instances, instanceId)
		if err != nil {
			return req.Fail(fmt.Sprintf("child(%s).managerSocket.Close: %v", instanceId, err))
		}
	} else {
		parent.instances[instanceId].status = status
	}

	return req.Ok(key_value.Empty())
}

// Run the instance manager to receive the data from the instances
func (parent *Parent) Run() {
	sock, err := zmq.NewSocket(zmq.PULL)
	if err != nil {
		parent.status = fmt.Sprintf("new_socket: %v", err)
		return
	}

	err = sock.Bind(config.ParentUrl(parent.id))
	if err != nil {
		parent.status = fmt.Sprintf("bind: %v", err)
		return
	}

	parent.status = Running
	parent.close = false

	poller := zmq.NewPoller()
	poller.Add(sock, zmq.POLLIN)

	for {
		if parent.close && len(parent.instances) == 0 {
			break
		}

		sockets, err := poller.Poll(time.Millisecond * 10)
		if err != nil {
			parent.status = fmt.Sprintf("poller.Poll: %v", err)
			break
		}

		if len(sockets) == 0 {
			continue
		}

		raw, err := sock.RecvMessage(0)
		if err != nil {
			parent.status = fmt.Sprintf("managerSocket.RecvMessage: %v", err)
			break
		}

		req, err := message.NewReq(raw)
		if err != nil {
			parent.status = fmt.Sprintf("message.NewRaw(%s): %v", raw, req)
			break
		}

		if req.Command != "set_status" {
			parent.status = fmt.Sprintf("command '%s' not supported", req.Command)
			break
		}

		reply := parent.onInstanceStatus(*req)
		if !reply.IsOK() {
			parent.status = fmt.Sprintf("onInstanceStatus: %s [%v]", reply.Message, req.Parameters)
			break
		}
	}

	err = poller.RemoveBySocket(sock)
	if err != nil {
		parent.status = fmt.Sprintf("poller.RemoveBySocket: %v", err)
		return
	}

	err = sock.Close()
	if err != nil {
		parent.status = fmt.Sprintf("managerSocket.Close: %v", err)
		return
	}

	parent.status = Idle
	parent.close = false
}

func (parent *Parent) Close() {
	parent.close = true
	// removing all running instances
	for instanceId := range parent.instances {
		err := parent.DeleteInstance(instanceId)
		if err != nil {
			parent.status = fmt.Sprintf("parent.DeleteInstance(%s): %v", instanceId, err)
			break
		}
	}
}

// AddInstance to the handler
func (parent *Parent) AddInstance(handlerType config.HandlerType, routes kvRef, routeDeps kvRef, clients kvRef) (string, error) {
	if parent.Status() != Running {
		return "", fmt.Errorf("instance_manager is not running. unexpected status: %s", parent.Status())
	}

	instanceNum := parent.lastInstanceId + 1
	id := fmt.Sprintf("%s_instance_%d", parent.id, instanceNum)

	added := instance.New(handlerType, id, parent.id, parent.logger)
	added.SetRoutes(routes, routeDeps)
	added.SetClients(clients)

	childSock, err := zmq.NewSocket(zmq.REQ)
	if err != nil {
		return id, fmt.Errorf("new childSocket(%s): %v", id, err)
	}
	err = childSock.Connect(config.InstanceUrl(parent.id, id))
	if err != nil {
		return id, fmt.Errorf("bind childSocket(%s): %v", id, err)
	}

	parent.instances[id] = &Child{
		status:        InstanceCreated,
		managerSocket: childSock,
	}
	go added.Run()

	// Make sure that instance is initialized
	go func(parent *Parent, instanceId string) {
		time.Sleep(time.Millisecond * 200)
		if parent.instances[instanceId] == nil || parent.instances[instanceId].status == InstanceCreated {
			parent.logger.Warn("instance not initialized", "instance id", instanceId)
			delete(parent.instances, instanceId)
		}
	}(parent, id)

	return id, nil
}

// DeleteInstance sends a signal to close the instance
func (parent *Parent) DeleteInstance(instanceId string) error {
	child, ok := parent.instances[instanceId]
	if !ok {
		return fmt.Errorf("instance[%s] not found", instanceId)
	}
	if child == nil {
		return fmt.Errorf("instance[%s] is null", instanceId)
	}
	if child.status == instance.CLOSED {
		return fmt.Errorf("instance[%s] is closed", instanceId)
	}

	req := message.Request{
		Command:    "close",
		Parameters: key_value.Empty(),
	}
	reqStr, err := req.String()
	if err != nil {
		return fmt.Errorf("req.String: %w", err)
	}

	_, err = child.managerSocket.SendMessage(reqStr)
	if err != nil {
		return fmt.Errorf("child(%s).SendMessage(%s): %w", instanceId, reqStr, err)
	}

	replyStr, err := child.managerSocket.RecvMessage(0)
	if err != nil {
		return fmt.Errorf("child(%s).RecvMessage: %w", instanceId, err)
	}
	reply, err := message.ParseReply(replyStr)
	if err != nil {
		return fmt.Errorf("parseReply(%s): %w", replyStr, err)
	}

	if !reply.IsOK() {
		return fmt.Errorf("instance close failed: %s", reply.Message)
	}

	return nil
}

// Instances returns all instances as instance_id => instance_status
func (parent *Parent) Instances() map[string]string {
	instances := make(map[string]string, len(parent.instances))

	for id, child := range parent.instances {
		instances[id] = child.status
	}

	return instances
}
