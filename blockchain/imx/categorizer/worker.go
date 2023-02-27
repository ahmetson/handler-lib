// imx smartcontract cateogirzer
// for documentation see:
// https://github.com/immutable/imx-core-sdk-golang/blob/6541766b54733580889f5051653d82f077c2aa17/imx/api/docs/TransfersApi.md#ListTransfers
// https://github.com/immutable/imx-core-sdk-golang/blob/6541766b54733580889f5051653d82f077c2aa17/imx/api/docs/MintsApi.md#listmints
package categorizer

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/blocklords/gosds/blockchain/imx"
	"github.com/blocklords/gosds/blockchain/imx/util"
	"github.com/blocklords/gosds/categorizer/log"
	"github.com/blocklords/gosds/categorizer/smartcontract"

	imx_api "github.com/immutable/imx-core-sdk-golang/imx/api"
)

// we fetch transfers and mints.
// each will slow down the sleep time to the IMX open client API.
const IMX_REQUEST_TYPE_AMOUNT = 2

// Run the goroutine for each Imx smartcontract.
func (manager *Manager) categorize(sm *smartcontract.Smartcontract) {
	configuration := imx_api.NewConfiguration()
	apiClient := imx_api.NewAPIClient(configuration)

	for {
		timestamp := time.Unix(int64(sm.CategorizedBlockTimestamp), 0).Format(time.RFC3339)

		_, err := categorize_imx_transfers(sm, apiClient, manager.DelayPerSecond, timestamp)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error when calling `Imx.TransfersApi.ListTransfers``: %v\n", err)
			fmt.Println("trying to request again in 10 seconds...")
			time.Sleep(10 * time.Second)
			continue
		}

		// it should be mints
		_, err = categorize_imx_mints(sm, apiClient, manager.DelayPerSecond, timestamp)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error when calling `Imx.TransfersApi.ListTransfers``: %v\n", err)
			fmt.Println("trying to request again in 10 seconds...")
			time.Sleep(10 * time.Second)
			continue
		}

	}
}

// Returns list of transfers
func categorize_imx_transfers(sm *smartcontract.Smartcontract, apiClient *imx_api.APIClient, sleep time.Duration, timestamp string) ([]*log.Log, error) {
	status := "success"
	pageSize := imx.PAGE_SIZE
	orderBy := "transaction_id"
	direction := "asc"

	cursor := ""
	var resp *imx_api.ListTransfersResponse
	var r *http.Response
	var err error
	broadcastTransactions := make([]*log.Log, 0)

	for {
		request := apiClient.TransfersApi.ListTransfers(context.Background()).MinTimestamp(timestamp).PageSize(pageSize)
		if cursor != "" {
			request = request.Cursor(cursor)
		}
		resp, r, err = request.OrderBy(orderBy).Direction(direction).Status(status).TokenAddress(sm.Address).Execute()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error when calling `Imx.TransfersApi.ListTransfers``: %v\n", err)
			fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
			fmt.Println("trying to request again in 10 seconds...")
			time.Sleep(10 * time.Second)
			return nil, fmt.Errorf("failed to fetch transfers list: %v", err)
		}

		for _, imxTx := range resp.Result {
			blockTime, err := time.Parse(time.RFC3339, imxTx.GetTimestamp())
			if err != nil {
				return nil, fmt.Errorf("error, parsing transaction data error: %v", err)
			}

			// eth transfers are not supported yet
			if imxTx.Token.Type == "ETH" {
				fmt.Println("skip, the SDS doesn't support transfer of ETH native tokens")
				continue
			}

			var arguments = make(map[string]interface{}, 3)
			arguments["from"] = imxTx.User
			arguments["to"] = imxTx.Receiver

			if imxTx.Token.Type == "ERC721" {
				arguments["tokenId"] = imxTx.Token.Data.TokenId
			} else {
				value, err := util.Erc20Amount(&imxTx.Token.Data)
				if err != nil {
					return nil, err
				}

				arguments["value"] = value
			}

			// todo change the imx to store in the log
			tx := &log.Log{
				NetworkId:      "imx",
				Address:        sm.Address,
				BlockNumber:    uint64(blockTime.Unix()),
				BlockTimestamp: uint64(blockTime.Unix()),
				Txid:           strconv.Itoa(int(imxTx.TransactionId)),
				LogIndex:       uint(0),
				// TxFrom:         imxTx.User,
				// Method:         "Transfer",
				// Args:           arguments,
				// Value:          0.0,
			}

			// todo broadcast to categorizer about a new log
			// and update smartcontract block number

			// if mints or transfers update the same block number
			// we skip it.
			// if int(blockTime.Unix()) > int(worker.Smartcontract.CategorizedBlockNumber) {
			// smartcontract.SetSyncing(worker.Db, worker.Smartcontract, uint64(blockTime.Unix()), uint64(blockTime.Unix()))
			// }

			broadcastTransactions = append(broadcastTransactions, tx)
		}

		time.Sleep(sleep * IMX_REQUEST_TYPE_AMOUNT)

		if resp.Remaining == 0 {
			break
		} else if cursor != "" {
			cursor = resp.Cursor
		}
	}

	return broadcastTransactions, nil
}

func categorize_imx_mints(sm *smartcontract.Smartcontract, apiClient *imx_api.APIClient, sleep time.Duration, timestamp string) ([]*log.Log, error) {
	status := "success"
	pageSize := imx.PAGE_SIZE
	orderBy := "transaction_id"
	direction := "asc"

	cursor := ""
	var resp *imx_api.ListTransfersResponse
	var r *http.Response
	var err error
	broadcastTransactions := make([]*log.Log, 0)

	for {
		request := apiClient.TransfersApi.ListTransfers(context.Background()).MinTimestamp(timestamp).PageSize(pageSize)
		if cursor != "" {
			request = request.Cursor(cursor)
		}
		resp, r, err = request.OrderBy(orderBy).Direction(direction).Status(status).TokenAddress(sm.Address).Execute()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error when calling `Imx.TransfersApi.ListTransfers``: %v\n", err)
			fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
			fmt.Println("trying to request again in 10 seconds...")
			time.Sleep(10 * time.Second)
			return nil, fmt.Errorf("failed to fetch transfers list: %v", err)
		}

		for _, imxTx := range resp.Result {
			blockTime, err := time.Parse(time.RFC3339, imxTx.GetTimestamp())
			if err != nil {
				return nil, fmt.Errorf("error, parsing transaction data error: %v", err)
			}

			// eth transfers are not supported yet
			if imxTx.Token.Type == "ETH" {
				fmt.Println("skip, the SDS doesn't support minting of ETH native tokens")
				continue
			}

			arguments := map[string]interface{}{
				"from": "0x0000000000000000000000000000000000000000",
				"to":   imxTx.User,
			}

			if imxTx.Token.Type == "ERC721" {
				arguments["tokenId"] = imxTx.Token.Data.TokenId
			} else {
				value, err := util.Erc20Amount(&imxTx.Token.Data)
				if err != nil {
					return nil, err
				}

				arguments["value"] = value
			}

			tx, err := log.NewFromMap(map[string]interface{}{
				"network_id":      "imx",
				"address":         sm.Address,
				"block_number":    uint64(blockTime.Unix()),
				"block_timestamp": uint64(blockTime.Unix()),
				"txid":            strconv.Itoa(int(imxTx.TransactionId)),
				"tx_index":        uint64(0),
				"tx_from":         imxTx.User,
				"method":          "Transfer",
				"arguments":       arguments,
				"value":           0.0,
			})
			if err != nil {
				fmt.Println("failed to parse the transaction. the imx response has ben changed")
				fmt.Println(err)
				continue
			}

			// if mints or transfers update the same block number
			// we skip it.
			// if int(blockTime.Unix()) > int(worker.Smartcontract.CategorizedBlockNumber) {
			// smartcontract.SetSyncing(worker.Db, worker.Smartcontract, uint64(blockTime.Unix()), uint64(blockTime.Unix()))
			// }
			// todo send to the categorizer

			broadcastTransactions = append(broadcastTransactions, tx)
		}
		time.Sleep(sleep * IMX_REQUEST_TYPE_AMOUNT)

		if resp.Remaining == 0 {
			break
		} else if cursor != "" {
			cursor = resp.Cursor
		}
	}

	return broadcastTransactions, nil
}
