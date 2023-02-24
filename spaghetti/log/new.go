package log

import (
	"encoding/hex"
	"errors"

	"github.com/blocklords/gosds/common/data_type/key_value"
	eth_types "github.com/ethereum/go-ethereum/core/types"
)

// Converts the ethereum's log to SeascapeSDS Spaghetti Log type
func NewFromRawLog(network_id string, block_timestamp uint64, log *eth_types.Log) (*Log, error) {
	topics := make([]string, len(log.Topics))
	for i, topic := range log.Topics {
		topics[i] = topic.Hex()
	}

	return &Log{
		NetworkId:      network_id,
		BlockNumber:    log.BlockHash.Big().Uint64(),
		BlockTimestamp: block_timestamp,
		Txid:           log.TxHash.Hex(),
		LogIndex:       log.Index,
		Data:           hex.EncodeToString(log.Data),
		Address:        log.Address.Hex(),
		Topics:         topics,
	}, nil
}

// Converts the ethereum's log to SeascapeSDS Spaghetti Log type
func NewLogsFromRaw(network_id string, block_timestamp uint64, raw_logs []eth_types.Log) ([]*Log, error) {
	logs := make([]*Log, 0, len(raw_logs))
	for i, raw := range raw_logs {
		log, err := NewFromRawLog(network_id, block_timestamp, &raw)
		if err != nil {
			return nil, err
		}

		logs[i] = log
	}

	return logs, nil
}

// Convert the JSON into spaghetti.Log
func New(parameters key_value.KeyValue) (*Log, error) {
	topics, err := parameters.GetStringList("topics")
	if err != nil {
		return nil, err
	}
	network_id, err := parameters.GetString("network_id")
	if err != nil {
		return nil, err
	}
	txid, err := parameters.GetString("txid")
	if err != nil {
		return nil, err
	}
	log_index, err := parameters.GetUint64("log_index")
	if err != nil {
		return nil, err
	}
	data, err := parameters.GetString("data")
	if err != nil {
		return nil, err
	}
	address, err := parameters.GetString("address")
	if err != nil {
		return nil, err
	}

	block_timestamp, err := parameters.GetUint64("block_timestamp")
	if err != nil {
		return nil, err
	}
	block_number, err := parameters.GetUint64("block_number")
	if err != nil {
		return nil, err
	}

	return &Log{
		NetworkId:      network_id,
		Address:        address,
		Txid:           txid,
		BlockNumber:    block_number,
		BlockTimestamp: block_timestamp,
		LogIndex:       uint(log_index),
		Data:           data,
		Topics:         topics,
	}, nil
}

// Parse list of Logs into array of spaghetti.Log
func NewLogs(raw_logs []interface{}) ([]*Log, error) {
	logs := make([]*Log, len(raw_logs))
	for i, raw := range raw_logs {
		if raw == nil {
			continue
		}
		log_map, ok := raw.(map[string]interface{})
		if !ok {
			return nil, errors.New("the log is not a map")
		}
		l, err := New(log_map)
		if err != nil {
			return nil, err
		}
		logs[i] = l
	}
	return logs, nil
}