package replier

// Asynchronous replier

import (
	"fmt"
	clientConfig "github.com/ahmetson/client-lib/config"
	"github.com/ahmetson/common-lib/data_type/key_value"
	"github.com/ahmetson/common-lib/message"
	"github.com/ahmetson/handler-lib/base"
	"github.com/ahmetson/handler-lib/config"
	"github.com/ahmetson/log-lib"
	"runtime"
)

// Replier is the socket wrapper for the service.
type Replier struct {
	base              *base.Handler
	maxInstanceAmount int
}

// New creates an asynchronous replying server.
func New() *Replier {
	return &Replier{
		base:              base.New(),
		maxInstanceAmount: runtime.NumCPU(),
	}
}

// SetConfig adds the parameters of the server from the config.
func (c *Replier) SetConfig(controller *config.Handler) {
	controller.Type = config.ReplierType
	c.base.SetConfig(controller)
}

// SetLogger sets the logger.
func (c *Replier) SetLogger(parent *log.Logger) error {
	return c.base.SetLogger(parent)
}

// AddDepByService adds the config of the dependency. Intended to be called by Service not by developer
func (c *Replier) AddDepByService(dep *clientConfig.Client) error {
	return c.base.AddDepByService(dep)
}

// AddedDepByService returns true if the configuration exists
func (c *Replier) AddedDepByService(id string) bool {
	return c.base.AddedDepByService(id)
}

// DepIds return the list of extension names required by this server.
func (c *Replier) DepIds() []string {
	return c.base.DepIds()
}

// Route adds a route along with its handler to this server
func (c *Replier) Route(cmd string, handle any, depIds ...string) error {
	return c.base.Route(cmd, handle, depIds...)
}

// Type returns the handler type. If the configuration is not set, returns config.UnknownType.
func (c *Replier) Type() config.HandlerType {
	return config.ReplierType
}

// Close the handler
func (c *Replier) Close() error {
	return c.base.Close()
}

func (c *Replier) Status() string {
	return c.base.Status()
}

// Start the handler directly, not by goroutine
func (c *Replier) Start() error {
	onAddInstance := func(req message.Request) message.Reply {
		m := c.base

		if len(m.InstanceManager.Instances()) >= c.maxInstanceAmount {
			return req.Fail(fmt.Sprintf("max amount of instances (%d) reached", c.maxInstanceAmount))
		}

		instanceId, err := m.InstanceManager.AddInstance(m.Config.Type, &m.Routes, &m.RouteDeps, &m.DepClients)
		if err != nil {
			return req.Fail(fmt.Sprintf("instanceManager.AddInstance(%s): %v", m.Config.Type, err))
		}

		params := key_value.Empty().Set("instance_id", instanceId)
		return req.Ok(params)
	}

	if c.base.Manager == nil {
		return fmt.Errorf("handler manager not initiated. call SetConfig and SetLogger")
	}

	if err := c.base.Manager.Route(config.AddInstance, onAddInstance); err != nil {
		return fmt.Errorf("overwriting handler manager 'add_instance' failed: %w", err)
	}

	return c.base.Start()
}

// MaxInstanceAmount is specific to Replier.
// Returns instances amount that it can have.
// It matches to the CPU amount.
func (c *Replier) MaxInstanceAmount() uint {
	return uint(c.maxInstanceAmount)
}

//func unwrap(msg []string) (head string, tail []string) {
//	head = msg[0]
//	if len(msg) > 1 && msg[1] == "" {
//		tail = msg[2:]
//	} else {
//		tail = msg[1:]
//	}
//
//	return
//}
