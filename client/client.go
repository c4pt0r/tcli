package client

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"tcli/utils"

	"github.com/pkg/errors"
	pd "github.com/tikv/pd/client"
)

type Key []byte
type Value []byte

type KV struct {
	K Key
	V Value
}

type KVS []KV

type KVSFormatter int

const (
	TableFormat = iota + 1000
	JsonFormat
)

func (kvs KVS) Print() {
	formatter := TableFormat
	if r, ok := utils.SysVarGet(utils.SysVarPrintFormatKey); ok {
		if string(r) == "json" {
			formatter = JsonFormat
		}
	}
	switch formatter {
	case TableFormat:
		{
			if len(kvs) == 0 {
				return
			}
			data := [][]string{
				{"Key", "Value"},
			}
			for _, kv := range kvs {
				row := []string{string(kv.K), string(kv.V)}
				data = append(data, row)
			}
			utils.PrintTable(data)
			if len(kvs) > 1 {
				fmt.Printf("%d Records Found\n", len(kvs))
			} else {
				fmt.Printf("%d Record Found\n", len(kvs))
			}
		}
	case JsonFormat:
		{
			out, _ := json.MarshalIndent(kvs, "", " ")
			fmt.Println(string(out))
		}
	default:
		{
			for _, kv := range kvs {
				fmt.Println(kv.K, "\t=>\t", kv.V)
			}
		}
	}
}

// Global client instance, safe to use concurrently
var (
	_globalKvClient atomic.Value
)

func InitTiKVClient(pdAddrs []string, clientMode string) error {
	switch strings.ToLower(clientMode) {
	case "raw":
		kvClient := newRawKVClient(pdAddrs)
		_globalKvClient.Store(kvClient)
		return nil
	case "txn":
		kvClient := newTxnKVClient(pdAddrs)
		_globalKvClient.Store(kvClient)
		return nil
	default:
		return errors.Errorf("Unrecognized TiKV mode: %s", clientMode)
	}
}

func GetTiKVClient() Client {
	return _globalKvClient.Load().(Client)
}

// Make sure txnkvClient implements Client interface
var _ Client = (*txnkvClient)(nil)
var _ Client = (*rawkvClient)(nil)

type Client interface {
	GetClientMode() TiKV_MODE
	GetClusterID() string
	GetStores() ([]StoreInfo, error)
	GetPDClient() pd.Client

	Put(ctx context.Context, kv KV) error
	PutJson(ctx context.Context, kv KV) error
	// add
	BatchPut(ctx context.Context, kv []KV) error

	Get(ctx context.Context, k Key) (KV, error)
	Scan(ctx context.Context, prefix []byte) (KVS, int, error)

	Delete(ctx context.Context, k Key) error
	BatchDelete(ctx context.Context, kvs []KV) error
	DeletePrefix(ctx context.Context, prefix Key, limit int) (Key, int, error)
}

type TiKV_MODE int

const (
	RAW_CLIENT TiKV_MODE = 0
	TXN_CLIENT TiKV_MODE = 1
)

func (mode TiKV_MODE) String() string {
	switch mode {
	case RAW_CLIENT:
		return "TiKV Raw Mode"
	case TXN_CLIENT:
		return "TiKV Txn Mode"
	}
	return "unknown"
}

var (
	propertiesKey = "property"
)

type StoreInfo struct {
	ID            string
	Version       string
	Addr          string
	State         string
	StatusAddress string
}

type PDInfo struct {
	Name       string
	ClientURLs []string
}

func (StoreInfo) TableTitle() []string {
	return []string{"Store ID", "Version", "Address", "State", "Status Address"}
}

func (s *StoreInfo) Flatten() []string {
	return []string{s.ID, s.Version, s.Addr, s.State, s.StatusAddress}
}

func (s StoreInfo) String() string {
	return fmt.Sprintf("store_id:\"%s\" version:\"%s\" addr:\"%s\" state:\"%s\" status_addr:\"%s\"", s.ID, s.Version, s.Addr, s.State, s.StatusAddress)
}
