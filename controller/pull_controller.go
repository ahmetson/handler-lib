/*
Package controller defines the data type of the various server sockets.

Using the controller package, you can turn application to the servers.

The following types of controllers are supported:

  - Pull creates a puller for the service. Puller enables the inputs, but doesn't respond back.
  - Reply creates a replier for the service. Reply executes the messages and replies back to the caller.
  - Router creates a proxy/broker for the service. Router forwards the requests to other Router/Reply or Pull
*/
package controller

import (
	"fmt"
	"github.com/ahmetson/service-lib/configuration"

	"github.com/ahmetson/service-lib/log"

	zmq "github.com/pebbe/zmq4"
)

// NewPull creates a pull controller for the service.
func NewPull(logger log.Logger) (*Controller, error) {
	controllerLogger := logger.Child("controller", "type", configuration.PusherType)

	// Socket to talk to clients
	socket, err := zmq.NewSocket(zmq.PULL)
	if err != nil {
		return nil, fmt.Errorf("zmq.NewSocket: %w", err)
	}

	return &Controller{
		socket:         socket,
		logger:         controllerLogger,
		controllerType: configuration.PusherType,
	}, nil
}
