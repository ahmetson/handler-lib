// Generic Type package handles the common functions of multiple SDS Data structures.
package generic_type

import (
	"github.com/blocklords/gosds/account"
	categorizer_log "github.com/blocklords/gosds/categorizer/log"
	categorizer_smartcontract "github.com/blocklords/gosds/categorizer/smartcontract"
	categorizer_transaction "github.com/blocklords/gosds/categorizer/transaction"

	spaghetti_log "github.com/blocklords/gosds/spaghetti/log"
	spaghetti_transaction "github.com/blocklords/gosds/spaghetti/transaction"

	static_abi "github.com/blocklords/gosds/static/abi"
	static_configuration "github.com/blocklords/gosds/static/configuration"
	static_smartcontract "github.com/blocklords/gosds/static/smartcontract"
	static_smartcontract_key "github.com/blocklords/gosds/static/smartcontract/key"
)

type SDS_Data interface {
	*categorizer_log.Log | *categorizer_smartcontract.Smartcontract | *categorizer_transaction.Transaction |
		*spaghetti_log.Log | *spaghetti_transaction.Transaction | *static_abi.Abi |
		*static_configuration.Configuration | *static_smartcontract.Smartcontract |
		*account.Account

	ToJSON() map[string]interface{}
}

type SDS_String_Data interface {
	*static_smartcontract_key.Key
}

// Converts the data structs to the JSON objects (represented as a golang map) list.
// []map[string]interface{}
func ToMapList[V SDS_Data](list []V) []map[string]interface{} {
	map_list := make([]map[string]interface{}, len(list))
	for i, element := range list {
		map_list[i] = element.ToJSON()
	}

	return map_list
}

// Converts the data structs to the list of strings.
// []string
func ToStringList[V SDS_String_Data](list []V) []string {
	string_list := make([]string, len(list))
	for i, element := range list {
		string_list[i] = string(*element)
	}

	return string_list
}
