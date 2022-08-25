package kvcmds

import (
	"context"
	"strconv"

	"github.com/c4pt0r/tcli"
	"github.com/c4pt0r/tcli/client"
	"github.com/c4pt0r/tcli/utils"
	"github.com/magiconair/properties"
)

type ScanCmd struct{}

var _ tcli.Cmd = ScanCmd{}

func (c ScanCmd) Name() string    { return "scan" }
func (c ScanCmd) Alias() []string { return []string{"scan"} }
func (c ScanCmd) Help() string {
	return `Scan keys from start key, use "scan --help" for more details`
}

func (c ScanCmd) LongHelp() string {
	s := c.Help()
	s += `
Usage:
	scan <start key> <options>
Options:
	--limit=<limit>, default 100
	--key-only=<true|false>, default false
	--strict-prefix=<true|false>, default false
	--count-only=<true|false>, default false
Examples:
	# scan from "a", max 10 keys
	scan "a" --limit=10

	# scan from "a", max 10 keys, output key-only
	scan "a" --limit=10 --key-only=true

	# scan from "a", max 10 keys, output key-only, the result keys are strictly prefix
	scan "a" --limit=10 --strict-prefix

	# scan from "a", count the number of keys, max 10 keys
	scan "a" --limit=10 --count-only

	scan "a" --limit=10 --strict-prefix --key-only=true
	scan $head --limit=10 --key-only=true
`
	return s
}

func (c ScanCmd) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
		utils.OutputWithElapse(func() error {
			ic := utils.ExtractIshellContext(ctx)
			if len(ic.Args) < 1 {
				utils.Print(c.LongHelp())
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

var _ tcli.Cmd = ScanPrefixCmd{}

func (c ScanPrefixCmd) Name() string    { return "scanp" }
func (c ScanPrefixCmd) Alias() []string { return []string{"scanp"} }
func (c ScanPrefixCmd) Help() string {
	return `scan keys with prefix, equals to "scan [key prefix] strict-prefix=true"`
}
func (c ScanPrefixCmd) LongHelp() string {
	return c.Help()
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

var _ tcli.Cmd = HeadCmd{}

func (c HeadCmd) Name() string    { return "head" }
func (c HeadCmd) Alias() []string { return []string{"head"} }
func (c HeadCmd) Help() string {
	return `scan keys from $head, equals to "scan $head limit=N", usage: head <limit>`
}

func (c HeadCmd) LongHelp() string {
	return c.Help()
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
