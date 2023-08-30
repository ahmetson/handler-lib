package sync_replier

import (
	clientConfig "github.com/ahmetson/client-lib/config"
	"github.com/ahmetson/handler-lib/config"
	"github.com/ahmetson/handler-lib/trigger"
	"github.com/ahmetson/log-lib"
)

type Publisher struct {
	base        *trigger.Trigger
	handlerType config.HandlerType
}

// New Publisher returned
func New() *Publisher {
	handler := trigger.New()
	return &Publisher{
		base:        handler,
		handlerType: config.PublisherType,
	}
}

// SetConfig adds the parameters of the server from the config.
func (c *Publisher) SetConfig(controller *config.Trigger) {
	controller.Type = config.PublisherType
	c.base.SetConfig(controller)
}

// SetLogger sets the logger.
func (c *Publisher) SetLogger(parent *log.Logger) error {
	return c.base.SetLogger(parent)
}

// AddDepByService adds the config of the dependency. Intended to be called by Service not by developer
func (c *Publisher) AddDepByService(dep *clientConfig.Client) error {
	return c.base.AddDepByService(dep)
}

// AddedDepByService returns true if the configuration exists
func (c *Publisher) AddedDepByService(id string) bool {
	return c.base.AddedDepByService(id)
}

// DepIds return the list of extension names required by this server.
func (c *Publisher) DepIds() []string {
	return c.base.DepIds()
}

// Route adds a route along with its handler to this server
func (c *Publisher) Route(cmd string, handle any, depIds ...string) error {
	return c.base.Route(cmd, handle, depIds...)
}

// Type returns the handler type. If the configuration is not set, returns config.UnknownType.
func (c *Publisher) Type() config.HandlerType {
	return config.PublisherType
}

// Close the handler
func (c *Publisher) Close() error {
	return c.base.Close()
}

func (c *Publisher) Status() string {
	return c.base.Status()
}

// Start the handler directly, not by goroutine
func (c *Publisher) Start() error {
	return c.base.Start()
}
