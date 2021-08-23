package kvcmds

import (
	"context"
	"fmt"
	"tcli/client"
	"tcli/utils"

	"github.com/abiosoft/ishell"
	"github.com/magiconair/properties"
)

type ScanCmd struct{}

func NewScanCmd() ScanCmd {
	return ScanCmd{}
}

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
			ic := ctx.Value("ishell").(*ishell.Context)
			if len(ic.Args) < 1 {
				fmt.Println(c.Help())
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

			kvs, err := client.GetTikvClient().Scan(utils.ContextWithProp(context.TODO(), scanOpt), startKey)
			if err != nil {
				return err
			}
			kvs.Print(client.TableFormat)
			return nil
		})
	}
}
