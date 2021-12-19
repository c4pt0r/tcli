package kvcmds

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"tcli"
	"tcli/client"
	"tcli/utils"

	"github.com/c4pt0r/log"
	"github.com/magiconair/properties"
)

type LoadCsvCmd struct{}

func (c LoadCsvCmd) Name() string    { return "loadcsv" }
func (c LoadCsvCmd) Alias() []string { return []string{"lcsv"} }
func (c LoadCsvCmd) Help() string {
	return `loadfile [filename] [key prefix] [opts], only supports CSV now, when "key prefix" is set, will automatically add prefix to the original key,
	                 opts:
			           batch-size: int, how many records in one tikv transaction, default: 1000`

}

func (c LoadCsvCmd) Suggest(line string) []tcli.CmdSuggest {
	return []tcli.CmdSuggest{}
}

func (c LoadCsvCmd) processCSV(prop *properties.Properties, rc io.Reader, keyPrefix []byte) error {
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

func (c LoadCsvCmd) Handler(ctx context.Context, input tcli.CmdInput) tcli.Result {
	log.D("load handler")
	return tcli.ResultOK
}
