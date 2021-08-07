package main

import (
	"fmt"
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
)

func (kvs KVS) Print(formatter KVSFormatter) {
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
			printTable(data)
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
	Put(kv KV) error
	BatchPut(kv []KV) error

	Get(k Key) (KV, error)
	Scan(prefix []byte, limit int) (KVS, error)

	Delete(k Key) error
	DeleteRange(start, end Key, opt ...interface{}) error
}
