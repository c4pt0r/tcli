package kvcmds

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"

	"github.com/c4pt0r/tcli/utils"

	"github.com/c4pt0r/tcli/client"

	"github.com/c4pt0r/tcli"

	"github.com/magiconair/properties"
)

type LoadCsvCmd struct{}

var _ tcli.Cmd = LoadCsvCmd{}

func (c LoadCsvCmd) Name() string    { return "loadcsv" }
func (c LoadCsvCmd) Alias() []string { return []string{"lcsv"} }
func (c LoadCsvCmd) Help() string {
	return `load csv file, use "loadcsv --help" for more details`

}

func (c LoadCsvCmd) LongHelp() string {
	s := c.Help()
	s += `
Usage:
	loadcsv [filename] <key prefix> <options>
Alias:
	lcsv
Options:
	--batch-size=<size>: int, how many records in one tikv transaction, default: 1000
Examples:
	# load csv file to tikv
	loadcsv sample.csv 

	# load csv file to tikv with key prefix: "prefix_"
	loadcsv sample.csv "prefix_" --batch-size=100

	# load csv file to tikv with key prefix and skip first row (header)
	loadcsv sample.csv "prefix_" --batch-size=100 --skip-rows=1
`
	return s
}

func (c LoadCsvCmd) processCSV(prop *properties.Properties, rc io.Reader, keyPrefix []byte) error {
	r := csv.NewReader(rc)
	var cnt int
	var batch []client.KV

	batchSize := prop.GetInt(tcli.LoadFileOptBatchSize, 1000)
	skips := prop.GetInt(tcli.LoadFileoptSkipRows, 0)
	for {
		rawRec, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if skips > 0 {
			skips--
			continue
		}
		if len(rawRec) != 2 {
			return fmt.Errorf("invalid csv record: %v, format should be: <key>,<value>", rawRec)
		}
		k, _ := utils.GetStringLit(rawRec[0])
		v, _ := utils.GetStringLit(rawRec[1])
		cnt++
		var key []byte
		if len(keyPrefix) > 0 {
			key = append([]byte{}, keyPrefix...)
			key = append(key, k...)
		} else {
			key = k
		}
		// TODO multi-threaded
		batch = append(batch, client.KV{
			K: key,
			V: v,
		})
		if len(batch) == batchSize {
			// do insert
			err := client.GetTiKVClient().BatchPut(context.TODO(), batch)
			if err != nil {
				return err
			}
			// Show progress
			progress := rc.(*utils.ProgressReader).GetProgress() * 100
			utils.Print(fmt.Sprintf("Progress: %d%% Count: %d Last Key: %s", int(progress), cnt, k))
			// clean buffer
			batch = nil
		}
	}
	// may have last batch
	if len(batch) > 0 {
		// do insert
		err := client.GetTiKVClient().BatchPut(context.TODO(), batch)
		if err != nil {
			return err
		}
	}
	utils.Print(fmt.Sprintf("Done, affected records: %d", cnt))
	return nil
}

func (c LoadCsvCmd) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
		utils.OutputWithElapse(func() error {
			var err error
			ic := utils.ExtractIshellContext(ctx)
			if len(ic.Args) == 0 {
				utils.Print(c.LongHelp())
				return nil
			}

			args, flags := utils.GetArgsAndOptionFlag(ic.RawArgs)

			// set filename
			var csvFile string
			if len(args) > 1 { // args[0] is the command name
				csvFile = args[1]
			}

			// set prefix
			var keyPrefix []byte
			if len(args) > 2 {
				var err error
				keyPrefix, err = utils.GetStringLit(args[2])
				if err != nil {
					return err
				}
			}

			// set prop
			prop := properties.NewProperties()
			if len(ic.Args) > 2 {
				err = utils.SetOptByString(flags, prop)
				if err != nil {
					return nil
				}
			}
			// open file for read
			fp, rdr, err := utils.OpenFileToProgressReader(csvFile)
			if err != nil {
				return err
			}
			defer fp.Close()
			// TODO should validate first
			// TODO set batch size
			return c.processCSV(prop, rdr, keyPrefix)
		})
	}
}
