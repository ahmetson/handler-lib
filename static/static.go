package static

import (
	"github.com/blocklords/sds/app/configuration"
	"github.com/blocklords/sds/app/controller"
	"github.com/blocklords/sds/app/log"
	"github.com/blocklords/sds/app/service"
	"github.com/blocklords/sds/db"
	"github.com/blocklords/sds/static/handler"
)

// Return the list of command handlers for this service
func CommandHandlers() controller.CommandHandlers {
	var commands = controller.CommandHandlers{
		"abi_get": handler.AbiGetBySmartcontractKey,
		"abi_set": handler.AbiRegister,

		"smartcontract_get":        handler.SmartcontractGet,
		"smartcontract_set":        handler.SmartcontractRegister,
		"smartcontract_filter":     handler.SmartcontractFilter,
		"smartcontract_key_filter": handler.SmartcontractKeyFilter,

		"configuration_get": handler.ConfigurationGet,
		"configuration_set": handler.ConfigurationRegister,
	}

	return commands
}

// Returns this service's configuration
func Service() *service.Service {
	return service.Inprocess(service.STATIC)
}

// Start the SDS Static core service.
// It keeps the static data:
// - smartcontract abi
// - smartcontract information
// - configuration (a relationship between common/topic.Topic and static.Smartcontract).
func Run(_ *configuration.Config, db_connection *db.Database) {
	logger, _ := log.New("static", log.WITH_TIMESTAMP)

	logger.Info("starting")

	// Getting the services which has access to the SDS Static
	static_env := Service()

	reply, err := controller.NewReply(static_env, logger)
	if err != nil {
		logger.Fatal("reply controller", "message", err)
	}

	err = reply.Run(CommandHandlers(), db_connection)
	if err != nil {
		logger.Fatal("reply controller", "message", err)
	}
}
