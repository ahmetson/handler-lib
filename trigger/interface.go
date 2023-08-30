package trigger

import (
	clientConfig "github.com/ahmetson/client-lib/config"
)

type Interface interface {
	TriggerClient() *clientConfig.Client
}
