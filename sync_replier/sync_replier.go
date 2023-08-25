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
func New() base.Interface {
	handler := base.New()
	return &SyncReplier{
		base:        handler,
		handlerType: config.SyncReplierType,
	}
}

// SetConfig adds the parameters of the server from the config.
func (c *SyncReplier) SetConfig(controller *config.Handler) {
	controller.Type = config.SyncReplierType
	c.base.SetConfig(controller)
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

// DepIds return the list of extension names required by this server.
func (c *SyncReplier) DepIds() []string {
	return c.base.DepIds()
}

// Route adds a route along with its handler to this server
func (c *SyncReplier) Route(cmd string, handle any, depIds ...string) error {
	return c.base.Route(cmd, handle, depIds...)
}

// Type returns the handler type. If the configuration is not set, returns config.UnknownType.
func (c *SyncReplier) Type() config.HandlerType {
	return config.SyncReplierType
}

// Close the handler
func (c *SyncReplier) Close() error {
	return c.base.Close()
}

func (c *SyncReplier) Status() string {
	return c.base.Status()
}

// Start the handler directly, not by goroutine
func (c *SyncReplier) Start() error {
	onAddInstance := func(req message.Request) message.Reply {
		m := c.base

		if len(m.InstanceManager.Instances()) > 1 {
			return req.Fail(fmt.Sprintf("only one instance allowed in sync replier"))
		}

		instanceId, err := m.InstanceManager.AddInstance(m.Config.Type, &m.Routes, &m.RouteDeps, &m.DepClients)
		if err != nil {
			return req.Fail(fmt.Sprintf("instanceManager.AddInstance(%s): %v", m.Config.Type, err))
		}

		params := key_value.Empty().Set("instance_id", instanceId)
		return req.Ok(params)
	}

	if err := c.base.Manager.Route("add_instance", onAddInstance); err != nil {
		return fmt.Errorf("overwriting handler manager 'add_instance' failed: %w", err)
	}

	return c.base.Start()
}
