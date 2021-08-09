package main

import (
	"context"
	"fmt"

	"github.com/abiosoft/ishell"
	"github.com/magiconair/properties"
)

var (
	ScanOptKeyOnly   string = "key-only"
	ScanOptCountOnly string = "count-only"
	ScanOptLimit     string = "limit"
)

type ScanCmd struct{}

func NewScanCmd() ScanCmd {
	return ScanCmd{}
}

func (c ScanCmd) Name() string    { return "scan" }
func (c ScanCmd) Alias() []string { return []string{"scan"} }
func (c ScanCmd) Help() string {
	return `Scan key-value pairs in range, usage: scan [start key] [opts]
                opt format: key1=value1,key2=value2,key3=value3, scan options:
                limit: integer, default:100
                key-only: true|false
                count-only: true|false`
}

func (c ScanCmd) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
		outputWithElapse(func() error {
			ic := ctx.Value("ishell").(*ishell.Context)
			if len(ic.Args) < 1 {
				fmt.Println(c.Help())
				return nil
			}
			s := ic.RawArgs[1]
			// it's a hex string literal
			_, startKey, err := getStringLit(s)
			if err != nil {
				return err
			}

			scanOpt := properties.NewProperties()
			if len(ic.Args) > 1 {
				err := setOptByString(ic.Args[1], scanOpt)
				if err != nil {
					return err
				}
			}

			kvs, err := GetTikvClient().Scan(contextWithProp(context.TODO(), scanOpt), startKey)
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

			err := GetTikvClient().Put(context.TODO(), KV{k, v})
			if err != nil {
				return err
			}
			return nil
		})
	}
}

type GetCmd struct{}

func (c GetCmd) Name() string    { return "get" }
func (c GetCmd) Alias() []string { return []string{".g"} }
func (c GetCmd) Help() string {
	return `get [string lit]`
}

func (c GetCmd) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
		outputWithElapse(func() error {
			ic := ctx.Value("ishell").(*ishell.Context)
			if len(ic.Args) < 1 {
				fmt.Println(c.Help())
				return nil
			}
			s := ic.RawArgs[1]
			// it's a hex string literal
			_, k, err := getStringLit(s)
			if err != nil {
				return err
			}
			kv, err := GetTikvClient().Get(context.TODO(), Key(k))
			if err != nil {
				return err
			}
			kvs :=[]KV{kv}
			KVS(kvs).Print(TableFormat)
			return nil
		})
	}
}


// EchoCmd is just for debugging
type EchoCmd struct{}

func (c EchoCmd) Name() string    { return "echo" }
func (c EchoCmd) Alias() []string { return []string{"echo"} }
func (c EchoCmd) Help() string {
	return `echo [string lit]`
}

func (c EchoCmd) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
		outputWithElapse(func() error {
			ic := ctx.Value("ishell").(*ishell.Context)
			if len(ic.Args) < 1 {
				fmt.Println(c.Help())
				return nil
			}
			s := ic.RawArgs[1]
			// it's a hex string literal
			_, v, err := getStringLit(s)
			if err != nil {
				return err
			}
			fmt.Println(string(v)) 
			return nil
		})
	}
}


