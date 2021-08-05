package main

import (
	"context"
)

type ScanCmd struct{}

func (c ScanCmd) Name() string    { return "scan" }
func (c ScanCmd) Alias() []string { return []string{"scan"} }
func (c ScanCmd) Help() string {
	return `scan key-value pairs in range, usage: scan [start key] [end key] [config1] [config2] ...
                config format: [config item]=[value]
                config items:

                offset : int, default: 0
                limit :  int, default: 100
                includeEdge: bool, default: true

                example: scan a z offset=0 limit=10 includeEdge=false`
}

func (c ScanCmd) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
		outputWithElapse(func() error {
			kvs, err := mc.Scan(nil, nil, 0, 0)
			if err != nil {
				return err
			}
			kvs.Print(TableFormat)
			return nil
		})
	}
}
