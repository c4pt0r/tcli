package main

import (
	"fmt"
	"os"

	"github.com/olekukonko/tablewriter"
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
			var data [][]string
			for _, kv := range kvs {
				row := []string{string(kv.K), string(kv.V)}
				data = append(data, row)
			}

			table := tablewriter.NewWriter(os.Stdout)
			table.SetHeader([]string{"Key", "Value"})

			table.SetBorders(tablewriter.Border{Left: true, Top: true, Right: true, Bottom: true})
			table.SetCenterSeparator("|")
			table.AppendBulk(data)
			table.Render()
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
	Scan(start, end Key, offset, limit int, flags ...interface{}) ([]KV, error)

	Delete(k Key) error
	DeleteRange(start, end Key, flags ...interface{}) error
}

type MockClient struct{}

func (c *MockClient) Put(kv KV) error {
	return nil
}
func (c *MockClient) BatchPut(kv []KV) error {
	return nil
}
func (c *MockClient) Get(k Key) (KV, error) {
	return KV{Key("hello"), Value("world")}, nil
}
func (c *MockClient) Scan(start, end Key, offset, limit int, flags ...interface{}) (KVS, error) {
	return []KV{
		KV{Key("2dfasdfasdf"), Value("b")},
		KV{Key("3dfdf"), Value("c")},
		KV{Key("4ddd"), Value("d")},
		KV{Key("dfdfdfdf"), Value(`Instead of rendering the table to io.Stdout you can also render it into a string. Go 1.10 introduced the strings.Builder type which implements the io.Writer interface and can therefore be used for this task. Example:`)},
	}, nil
}
func (c *MockClient) Delete(k Key) error {
	return nil
}
func (c *MockClient) DeleteRange(start, end Key, flags ...interface{}) error {
	return nil
}

var mc *MockClient = &MockClient{}
