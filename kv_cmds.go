package main

import (
	"context"
	"fmt"
	"strconv"

	"github.com/abiosoft/ishell"
)

type ScanCmd struct{}

func (c ScanCmd) Name() string    { return "scan" }
func (c ScanCmd) Alias() []string { return []string{"scan"} }
func (c ScanCmd) Help() string {
	return `scan key-value pairs in range, usage: scan [start key] [limit]`
}

func (c ScanCmd) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
		outputWithElapse(func() error {
			ic := ctx.Value("ishell").(*ishell.Context)
			if len(ic.Args) < 1 {
				fmt.Println(c.Help())
				return nil
			}
			startKey := []byte(ic.Args[0])

			limit := 100
			var err error
			if len(ic.Args) > 1 {
				limit, err = strconv.Atoi(ic.Args[1])
				if err != nil {
					return err
				}
			}
			kvs, err := GetTikvClient().Scan(startKey, limit)
			if err != nil {
				return err
			}
			kvs.Print(TableFormat)
			return nil
		})
	}
}

type PutCmd struct{}

func (c PutCmd) Name() string    { return "put" }
func (c PutCmd) Alias() []string { return []string{"put", "set"} }
func (c PutCmd) Help() string {
	return `put [key] [value]`
}

func (c PutCmd) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
		outputWithElapse(func() error {
			ic := ctx.Value("ishell").(*ishell.Context)
			if len(ic.Args) < 2 {
				fmt.Println(c.Help())
				return nil
			}
			k, v := []byte(ic.Args[0]), []byte(ic.Args[1])

			err := GetTikvClient().Put(KV{k, v})
			if err != nil {
				return err
			}
			return nil
		})
	}
}
