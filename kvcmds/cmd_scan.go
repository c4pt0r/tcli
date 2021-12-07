package kvcmds

import (
	"context"
	"strconv"
	"tcli"
	"tcli/client"
	"tcli/utils"

	"github.com/magiconair/properties"
)

type ScanCmd struct{}

func (c ScanCmd) Name() string    { return "scan" }
func (c ScanCmd) Alias() []string { return []string{"scan"} }
func (c ScanCmd) Help() string {
	return `Scan key-value pairs in range, usage: scan [start key] [opts]
                opt format: key1=value1,key2=value2,key3=value3, 
                scan options:
                  limit: integer, default:100
                  key-only: true(1)|false(0)
                  strict-prefix: true(1)|false(0)
                  count-only: true(1)|false(0)`
}

func (c ScanCmd) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
		utils.OutputWithElapse(func() error {
			ic := utils.ExtractIshellContext(ctx)
			if len(ic.Args) < 1 {
				utils.Print(c.Help())
				return nil
			}
			s := ic.RawArgs[1]
			// it's a hex string literal
			startKey, err := utils.GetStringLit(s)
			if err != nil {
				return err
			}
			scanOpt := properties.NewProperties()
			if len(ic.Args) > 1 {
				err := utils.SetOptByString(ic.Args[1:], scanOpt)
				if err != nil {
					return err
				}
			}
			kvs, _, err := client.GetTiKVClient().Scan(utils.ContextWithProp(context.TODO(), scanOpt), startKey)
			if err != nil {
				return err
			}
			kvs.Print()
			return nil
		})
	}
}

type ScanPrefixCmd struct{}

func (c ScanPrefixCmd) Name() string    { return "scanp" }
func (c ScanPrefixCmd) Alias() []string { return []string{"scanp"} }
func (c ScanPrefixCmd) Help() string {
	return `scan keys with prefix, equals to "scan [key prefix] strict-prefix=true"`
}

func (c ScanPrefixCmd) Handler() func(ctx context.Context) {
	// TODO need refactor
	return func(ctx context.Context) {
		utils.OutputWithElapse(func() error {
			ic := utils.ExtractIshellContext(ctx)
			if len(ic.Args) < 1 {
				utils.Print(c.Help())
				return nil
			}
			s := ic.RawArgs[1]
			// it's a hex string literal
			startKey, err := utils.GetStringLit(s)
			if err != nil {
				return err
			}
			scanOpt := properties.NewProperties()
			if len(ic.Args) > 1 {
				err := utils.SetOptByString(ic.Args[1:], scanOpt)
				if err != nil {
					return err
				}
			}
			scanOpt.Set(tcli.ScanOptStrictPrefix, "true")
			kvs, _, err := client.GetTiKVClient().Scan(utils.ContextWithProp(context.TODO(), scanOpt), startKey)
			if err != nil {
				return err
			}
			kvs.Print()
			return nil
		})
	}
}

type HeadCmd struct{}

func (c HeadCmd) Name() string    { return "head" }
func (c HeadCmd) Alias() []string { return []string{"head"} }
func (c HeadCmd) Help() string {
	return `scan keys from $head, equals to "scan $head limit=N", usage: head <limit>`
}

func (c HeadCmd) Handler() func(ctx context.Context) {
	// TODO need refactor
	return func(ctx context.Context) {
		utils.OutputWithElapse(func() error {
			ic := utils.ExtractIshellContext(ctx)
			if len(ic.Args) < 1 {
				utils.Print(c.Help())
				return nil
			}
			_, err := strconv.Atoi(ic.Args[0])
			if err != nil {
				return err
			}
			scanOpt := properties.NewProperties()
			if len(ic.Args) > 1 {
				err := utils.SetOptByString(ic.Args[1:], scanOpt)
				if err != nil {
					return err
				}
			}
			// set limit
			scanOpt.Set(tcli.ScanOptLimit, ic.Args[0])
			scanOpt.Set(tcli.ScanOptStrictPrefix, "false")
			kvs, _, err := client.GetTiKVClient().Scan(utils.ContextWithProp(context.TODO(), scanOpt), []byte("\x00"))
			if err != nil {
				return err
			}
			kvs.Print()
			return nil
		})
	}
}
