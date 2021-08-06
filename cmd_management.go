package main

import (
	"context"
	"fmt"

	"github.com/abiosoft/ishell"
)

type ConnectCmd struct{}

func (c ConnectCmd) Name() string    { return ".connect" }
func (c ConnectCmd) Alias() []string { return []string{".c", ".conn"} }
func (c ConnectCmd) Help() string {
	return "connect to a tikv cluster, usage: [.connect|.conn|.c] [pd addr], example: .c 192.168.1.1:2379"
}

func (c ConnectCmd) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
		ic := ctx.Value("ishell").(*ishell.Context)
		if len(ic.Args) > 0 {
			ic.Println("connecting to", ic.Args[0])
			ic.SetPrompt(fmt.Sprintf("[%s] >>> ", ic.Args[0]))
		} else {
			ic.Println(ic.Cmd.HelpText())
		}
	}
}

type PingCmd struct{}

func (c PingCmd) Name() string    { return ".ping" }
func (c PingCmd) Alias() []string { return []string{".p"} }
func (c PingCmd) Help() string {
	return "ping pd, usage: [.ping|.p] 192.168.1.1:2379"
}

func (c PingCmd) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
	}
}

type ListStoresCmd struct{}

func (c ListStoresCmd) Name() string    { return ".stores" }
func (c ListStoresCmd) Alias() []string { return []string{".stores"} }
func (c ListStoresCmd) Help() string {
	return "list tikv stores in cluster"
}

func (c ListStoresCmd) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
		outputWithElapse(func() error {
			stores, err := GetTikvClient().GetStores()
			if err != nil {
				return err
			}

			var output [][]string = [][]string{
				(StoreInfo).TableTitle(StoreInfo{}),
			}
			for _, store := range stores {
				output = append(output, store.Flatten())
			}
			printTable(output)
			return nil
		})
	}
}
