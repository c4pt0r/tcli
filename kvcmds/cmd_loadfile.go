package kvcmds

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"tcli"
	"tcli/client"
	"tcli/utils"

	"github.com/magiconair/properties"
)

type LoadFileCmd struct{}

func (c LoadFileCmd) Name() string    { return "loadfile" }
func (c LoadFileCmd) Alias() []string { return []string{"l"} }
func (c LoadFileCmd) Help() string {
	return `loadfile [filename] [key prefix] [opts], only supports CSV now, when "key prefix" is set, will automatically add prefix to the original key,
	                 opts:
			           batch-size: int, how many records in one tikv transaction, default: 1000`

}

func (c LoadFileCmd) processCSV(prop *properties.Properties, rc io.Reader, keyPrefix []byte) error {
	r := csv.NewReader(rc)
	if _, err := r.Read(); err != nil { //read header
		return err
	}
	var cnt int
	var batch []client.KV

	batchSize := prop.GetInt(tcli.LoadFileOptBatchSize, 1000)
	for {
		rawRec, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
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
			err := client.GetTikvClient().BatchPut(context.TODO(), batch)
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
		err := client.GetTikvClient().BatchPut(context.TODO(), batch)
		if err != nil {
			return err
		}
	}
	utils.Print(fmt.Sprintf("Done, affected records: %d", cnt))
	return nil
}

func (c LoadFileCmd) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
		utils.OutputWithElapse(func() error {
			var err error
			ic := utils.ExtractIshellContext(ctx)
			if len(ic.Args) == 0 {
				utils.Print(c.Help())
				return nil
			}

			// set filename
			var csvFile string
			if len(ic.Args) > 0 {
				csvFile = ic.Args[0]
			}

			// set prefix
			var keyPrefix []byte
			if len(ic.Args) > 1 && !(ic.RawArgs[2] == `""` || ic.RawArgs[2] == `''`) {
				keyPrefix, err = utils.GetStringLit(ic.RawArgs[2])
				if err != nil {
					return err
				}
			}

			// set prop
			prop := properties.NewProperties()
			if len(ic.Args) > 2 {
				err = utils.SetOptByString(ic.RawArgs[3:], prop)
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
