package config

import "fmt"

// HandlerType defines the available kind of handlers
type HandlerType string

// ReplierType or PublisherType or ReplierType
const (
	// SyncReplierType handlers process a one request at a time.
	SyncReplierType HandlerType = "SyncReplier"
	// PusherType handlers trigger the data but don't return the data.
	// If multiple instances of Pullers are connected. Then Pusher sends the data to one Puller in a round-robin
	// way.
	PusherType HandlerType = "Pusher"
	// PublisherType handlers broadcast the message to all subscribers. It's a trigger-able handler.
	PublisherType HandlerType = "Publisher"
	// ReplierType handlers are the asynchronous ReplierType. It's a traditional client-server's server.
	ReplierType HandlerType = "Replier"
	UnknownType HandlerType = ""
)

// IsValid checks whether the given string is the valid or not.
// If not valid, then returns the error otherwise returns nil.
func IsValid(t HandlerType) error {
	if t == SyncReplierType || t == PusherType || t == PublisherType || t == ReplierType {
		return nil
	}

	return fmt.Errorf("'%s' is not valid handler type", t)
}
