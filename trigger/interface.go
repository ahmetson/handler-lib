package trigger

import (
	clientConfig "github.com/ahmetson/client-lib/config"
	"github.com/ahmetson/handler-lib/config"
)

type Interface interface {
	TriggerClient() *clientConfig.Client
	Config() *config.Trigger
}
