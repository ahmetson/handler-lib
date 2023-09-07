package sync_replier

import (
	"fmt"
	clientConfig "github.com/ahmetson/client-lib/config"
	"github.com/ahmetson/common-lib/data_type/key_value"
	"github.com/ahmetson/common-lib/message"
	"github.com/ahmetson/handler-lib/base"
	"github.com/ahmetson/handler-lib/config"
	"github.com/ahmetson/log-lib"
)

type SyncReplier struct {
	base        *base.Handler
	handlerType config.HandlerType
}

// New SyncReplier returned
func New() *SyncReplier {
	handler := base.New()
	return &SyncReplier{
		base:        handler,
		handlerType: config.SyncReplierType,
	}
}

func (c *SyncReplier) Config() *config.Handler {
	return c.base.Config()
}

// SetConfig adds the parameters of the handler from the config.
func (c *SyncReplier) SetConfig(handler *config.Handler) {
	handler.Type = config.SyncReplierType
	c.base.SetConfig(handler)
}

// SetLogger sets the logger.
func (c *SyncReplier) SetLogger(parent *log.Logger) error {
	return c.base.SetLogger(parent)
}

// AddDepByService adds the config of the dependency. Intended to be called by Service not by developer
func (c *SyncReplier) AddDepByService(dep *clientConfig.Client) error {
	return c.base.AddDepByService(dep)
}

// AddedDepByService returns true if the configuration exists
func (c *SyncReplier) AddedDepByService(id string) bool {
	return c.base.AddedDepByService(id)
}

// DepIds return the list of extension names required by this handler.
func (c *SyncReplier) DepIds() []string {
	return c.base.DepIds()
}

// Route adds a route along with its handler to this handler
func (c *SyncReplier) Route(cmd string, handle any, depIds ...string) error {
	return c.base.Route(cmd, handle, depIds...)
}

// Type returns the handler type. If the configuration is not set, returns config.UnknownType.
func (c *SyncReplier) Type() config.HandlerType {
	return config.SyncReplierType
}

func (c *SyncReplier) Status() string {
	return c.base.Status()
}

// Start the handler directly, not by goroutine
func (c *SyncReplier) Start() error {
	onAddInstance := func(req message.Request) message.Reply {
		m := c.base

		if len(m.InstanceManager.Instances()) != 0 {
			return req.Fail(fmt.Sprintf("only one instance allowed in sync replier"))
		}

		instanceId, err := m.InstanceManager.AddInstance(m.Config().Type, &m.Routes, &m.RouteDeps, &m.DepClients)
		if err != nil {
			return req.Fail(fmt.Sprintf("instanceManager.AddInstance(%s): %v", m.Config().Type, err))
		}

		params := key_value.Empty().Set("instance_id", instanceId)
		return req.Ok(params)
	}

	if c.base.Manager == nil {
		return fmt.Errorf("handler manager not initited. call SetConfig and SetLogger first")
	}

	if err := c.base.Manager.Route(config.AddInstance, onAddInstance); err != nil {
		return fmt.Errorf("overwriting handler manager 'add_instance' failed: %w", err)
	}

	return c.base.Start()
}
