package replier

// Asynchronous replier

import (
	"fmt"
	"github.com/ahmetson/datatype-lib/data_type/key_value"
	"github.com/ahmetson/datatype-lib/message"
	"github.com/ahmetson/handler-lib/base"
	"github.com/ahmetson/handler-lib/config"
	"runtime"
)

// Replier is the socket wrapper for the service.
type Replier struct {
	*base.Handler
	maxInstanceAmount int
}

// New asynchronous replying handler.
func New() *Replier {
	return &Replier{
		Handler:           base.New(),
		maxInstanceAmount: runtime.NumCPU(),
	}
}

// SetConfig adds the parameters of the handler from the config.
func (c *Replier) SetConfig(handler *config.Handler) {
	handler.Type = config.ReplierType
	c.Handler.SetConfig(handler)
}

// Type returns the handler type. If the configuration is not set, returns config.UnknownType.
func (c *Replier) Type() config.HandlerType {
	return config.ReplierType
}

// Start the handler directly, not by goroutine
func (c *Replier) Start() error {
	onAddInstance := func(req message.RequestInterface) message.ReplyInterface {
		if len(c.InstanceManager.Instances()) >= c.maxInstanceAmount {
			return req.Fail(fmt.Sprintf("max amount of instances (%d) reached", c.maxInstanceAmount))
		}

		instanceId, err := c.InstanceManager.AddInstance(c.Config().Type, &c.Routes, &c.RouteDeps, &c.DepClients)
		if err != nil {
			return req.Fail(fmt.Sprintf("instanceManager.AddInstance(%s): %v", c.Config().Type, err))
		}

		params := key_value.New().Set("instance_id", instanceId)
		return req.Ok(params)
	}

	if c.Manager == nil {
		return fmt.Errorf("handler manager not initiated. call SetConfig and SetLogger")
	}

	if err := c.Manager.Route(config.AddInstance, onAddInstance); err != nil {
		return fmt.Errorf("overwriting handler manager 'add_instance' failed: %w", err)
	}

	return c.Start()
}

// MaxInstanceAmount is specific to Replier.
// Returns instances amount that it can have.
// It matches to the CPU amount.
func (c *Replier) MaxInstanceAmount() uint {
	return uint(c.maxInstanceAmount)
}
