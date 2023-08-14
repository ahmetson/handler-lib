package config

import "fmt"

// HandlerType defines the available kind of controllers
type HandlerType string

// ReplierType or PublisherType or ReplierType
const (
	// SyncReplierType controllers are serving one request at a time. It's the server in a
	// traditional client-server model.
	SyncReplierType HandlerType = "SyncReplier"
	// PusherType controllers are serving the data to the Pullers without checking its delivery.
	// If multiple instances of Pullers are connected. Then Pusher sends the data to one Puller in a round-robin
	// way.
	PusherType HandlerType = "Pusher"
	// PublisherType controllers are broadcasting the message to all subscribers
	PublisherType HandlerType = "Publisher"
	// ReplierType controllers are the asynchronous ReplierType
	ReplierType HandlerType = "Replier"
	UnknownType HandlerType = ""
)

// ValidateControllerType checks whether the given string is the valid or not.
// If not valid, then returns the error otherwise returns nil.
func ValidateControllerType(t HandlerType) error {
	if t == SyncReplierType || t == PusherType || t == PublisherType || t == ReplierType {
		return nil
	}

	return fmt.Errorf("'%s' is not valid server type", t)
}
