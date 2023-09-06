// Package manager_client creates a client that interacts with the handler manager.
// Useful for calling it from the service.
package manager_client

import (
	"fmt"
	"github.com/ahmetson/client-lib"
	"github.com/ahmetson/common-lib/data_type/key_value"
	"github.com/ahmetson/common-lib/message"
	handlerConfig "github.com/ahmetson/handler-lib/config"
	"github.com/ahmetson/handler-lib/handler_manager"
	"time"
)

type Client struct {
	socket *client.Socket
	config *handlerConfig.Handler
}

type Interface interface {
	// Close socket itself
	Close() error
	Timeout(duration time.Duration)
	Attempt(uint8)

	// HandlerStatus of the handler. Return status. If the status is incomplete, then returns part statuses
	HandlerStatus() (string, key_value.KeyValue, error)

	// ClosePart stops the running handler part.
	// Part could be 'frontend,' 'instance_manager,' optionally a 'broadcaster'
	ClosePart(part string) error

	// RunPart runs the handler part. If the handler part was running already, returns an error.
	// Part could be 'frontend,' 'instance_manager,' optionally a 'broadcaster'
	RunPart(part string) error

	// InstanceAmount that are currently running
	InstanceAmount() (uint8, error)

	// MessageAmount returns the messages in each part.
	// Returns queue_length, processing_length, and optionally broadcasting_length
	MessageAmount() (key_value.KeyValue, error)

	// AddInstance adds a new instance. If successfully added, returns its id.
	AddInstance() (string, error)

	// DeleteInstance removes the running instance by its id.
	DeleteInstance(instanceId string) error

	// Parts returns the available parts and message types
	Parts() ([]string, []string, error)
}

// New client that's connected to the handler
func New(configHandler *handlerConfig.Handler) (Interface, error) {
	socketType := handlerConfig.SocketType(configHandler.Type)
	url := handlerConfig.ManagerUrl(configHandler.Id)
	socket, err := client.NewRaw(socketType, url)
	if err != nil {
		return nil, fmt.Errorf("client.New: %w", err)
	}

	return &Client{socket: socket, config: configHandler}, nil
}

// Timeout of the client socket
func (c *Client) Timeout(duration time.Duration) {
	c.socket.Timeout(duration)
}

// Attempt amount for requests
func (c *Client) Attempt(attempt uint8) {
	c.socket.Attempt(attempt)
}

func (c *Client) Close() error {
	return c.socket.Close()
}

// HandlerStatus returns the handler status.
// If the handler is not ready, then optionally returns parts.
func (c *Client) HandlerStatus() (string, key_value.KeyValue, error) {
	req := message.Request{
		Command:    handlerConfig.HandlerStatus,
		Parameters: key_value.Empty(),
	}

	reply, err := c.socket.Request(&req)
	if err != nil {
		return "", nil, fmt.Errorf("socket.Request('%s'): %w", handlerConfig.HandlerStatus, err)
	}
	if !reply.IsOK() {
		return "", nil, fmt.Errorf("reply.Message: %s", reply.Message)
	}

	status, err := reply.Parameters.GetString("status")
	if err != nil {
		return "", nil, fmt.Errorf("reply.Parameters.GetString('status'): %w", err)
	}
	parts := key_value.Empty()
	if status != handler_manager.Ready {
		parts, err = reply.Parameters.GetKeyValue("parts")
		if err != nil {
			return "", nil, fmt.Errorf("reply.Parameters.GetKeyValue('parts'): %w", err)
		}
	}

	return status, parts, nil
}

func (c *Client) Parts() ([]string, []string, error) {
	req := message.Request{
		Command:    handlerConfig.Parts,
		Parameters: key_value.Empty(),
	}

	reply, err := c.socket.Request(&req)
	if err != nil {
		return nil, nil, fmt.Errorf("socket.Request('%s'): %w", handlerConfig.HandlerStatus, err)
	}
	if !reply.IsOK() {
		return nil, nil, fmt.Errorf("reply.Message: %s", reply.Message)
	}

	parts, err := reply.Parameters.GetStringList("parts")
	if err != nil {
		return nil, nil, fmt.Errorf("reply.Parameters.GetString('parts'): %w", err)
	}
	messageTypes, err := reply.Parameters.GetStringList("message_types")
	if err != nil {
		return nil, nil, fmt.Errorf("reply.Parameters.GetString('message_types'): %w", err)
	}

	return parts, messageTypes, nil
}

// ClosePart closes the part of the handler.
func (c *Client) ClosePart(part string) error {
	req := message.Request{
		Command:    handlerConfig.ClosePart,
		Parameters: key_value.Empty().Set("part", part),
	}

	reply, err := c.socket.Request(&req)
	if err != nil {
		return fmt.Errorf("socket.Request('%s'): %w", handlerConfig.ClosePart, err)
	}
	if !reply.IsOK() {
		return fmt.Errorf("reply.Message: %s", reply.Message)
	}

	return nil
}

// RunPart runs the handler part. If the handler part was running already, returns an error.
// Part could be 'frontend,' 'instance_manager,' optionally a 'broadcaster'
func (c *Client) RunPart(part string) error {
	req := message.Request{
		Command:    handlerConfig.RunPart,
		Parameters: key_value.Empty().Set("part", part),
	}

	reply, err := c.socket.Request(&req)
	if err != nil {
		return fmt.Errorf("socket.Request('%s'): %w", handlerConfig.RunPart, err)
	}
	if !reply.IsOK() {
		return fmt.Errorf("reply.Message: %s", reply.Message)
	}

	return nil
}

// InstanceAmount that are currently running
func (c *Client) InstanceAmount() (uint8, error) {
	req := message.Request{
		Command:    handlerConfig.InstanceAmount,
		Parameters: key_value.Empty(),
	}

	reply, err := c.socket.Request(&req)
	if err != nil {
		return 0, fmt.Errorf("socket.Request('%s'): %w", handlerConfig.InstanceAmount, err)
	}
	if !reply.IsOK() {
		return 0, fmt.Errorf("reply.Message: %s", reply.Message)
	}

	instanceAmount, err := reply.Parameters.GetUint64("instance_amount")
	if err != nil {
		return 0, fmt.Errorf("reply.Parameters.GetUint('instance_amount'): %w", err)
	}

	return uint8(instanceAmount), nil
}

// MessageAmount returns the messages in each part.
// Returns queue_length, processing_length, and optionally broadcasting_length
func (c *Client) MessageAmount() (key_value.KeyValue, error) {
	req := message.Request{
		Command:    handlerConfig.MessageAmount,
		Parameters: key_value.Empty(),
	}

	reply, err := c.socket.Request(&req)
	if err != nil {
		return nil, fmt.Errorf("socket.Request('%s'): %w", handlerConfig.MessageAmount, err)
	}
	if !reply.IsOK() {
		return nil, fmt.Errorf("reply.Message: %s", reply.Message)
	}

	if len(reply.Parameters) == 0 {
		return nil, fmt.Errorf("reply.Parameters is empty")
	}

	return reply.Parameters, nil
}

// AddInstance adds a new instance. If successfully added, returns its id.
func (c *Client) AddInstance() (string, error) {
	req := message.Request{
		Command:    handlerConfig.AddInstance,
		Parameters: key_value.Empty(),
	}

	reply, err := c.socket.Request(&req)
	if err != nil {
		return "", fmt.Errorf("socket.Request('%s'): %w", handlerConfig.AddInstance, err)
	}
	if !reply.IsOK() {
		return "", fmt.Errorf("reply.Message: %s", reply.Message)
	}

	instanceId, err := reply.Parameters.GetString("instance_id")
	if err != nil {
		return "", fmt.Errorf("reply.Parameters.GetString('instance_id'): %w", err)
	}

	return instanceId, nil
}

// DeleteInstance removes the running instance by its id.
func (c *Client) DeleteInstance(instanceId string) error {
	req := message.Request{
		Command:    handlerConfig.DeleteInstance,
		Parameters: key_value.Empty().Set("instance_id", instanceId),
	}

	reply, err := c.socket.Request(&req)
	if err != nil {
		return fmt.Errorf("socket.Request('%s'): %w", handlerConfig.DeleteInstance, err)
	}
	if !reply.IsOK() {
		return fmt.Errorf("reply.Message: %s", reply.Message)
	}

	return nil
}
