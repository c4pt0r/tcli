package client

import (
	"context"
	"encoding/json"
	"fmt"
	"tcli/utils"
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

type Client interface {
	Put(ctx context.Context, kv KV) error
	BatchPut(ctx context.Context, kv []KV) error

	Get(ctx context.Context, k Key) (KV, error)
	Scan(ctx context.Context, prefix []byte) (KVS, int, error)

	Delete(ctx context.Context, k Key) error
	BatchDelete(ctx context.Context, kvs []KV) error
	DeletePrefix(ctx context.Context, prefix Key, limit int) (Key, int, error)
}
