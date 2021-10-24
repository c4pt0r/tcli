package kvcmds

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"os"
	"tcli"
	"tcli/client"
	"tcli/utils"

	"github.com/magiconair/properties"
)

type BackupCmd struct{}

func (c BackupCmd) Name() string    { return "backup" }
func (c BackupCmd) Alias() []string { return []string{"backup"} }
func (c BackupCmd) Help() string {
	return `backup <prefix> <outfile> <opts>, opts: type=csv batch-size=1000(default) concurrent=1, example: backup * outfile`
}

func writeKvsToCsvFile(w *csv.Writer, kvs client.KVS) error {
	for _, kv := range kvs {
		line := []string{utils.Bytes2StrLit(kv.K), utils.Bytes2StrLit(kv.V)}
		err := w.Write(line)
		if err != nil {
			return err
		}
	}
	w.Flush()
	return nil
}

func (c BackupCmd) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
		utils.OutputWithElapse(func() error {
			ic := utils.ExtractIshellContext(ctx)
			if len(ic.Args) < 2 {
				utils.Print(c.Help())
				return nil
			}
			prefix, err := utils.GetStringLit(ic.Args[0])
			if err != nil {
				return err
			}
			outputFile := ic.Args[1]
			_, err = os.Stat(outputFile)
			if !os.IsNotExist(err) {
				return errors.New("Backup file already exists")
			}
			fp, err := os.Create(outputFile)
			if err != nil {
				return err
			}
			csvWriter := csv.NewWriter(fp)
			defer csvWriter.Flush()
			// Write first line
			csvWriter.Write([]string{"Key", "Value"})

			opt := properties.NewProperties()
			if len(ic.Args) > 1 {
				err := utils.SetOptByString(ic.Args[1:], opt)
				if err != nil {
					return err
				}
			}
			opt.Set(tcli.ScanOptLimit, opt.GetString(tcli.BackupOptBatchSize, "1000"))
			if bytes.Compare(prefix, []byte("\x00")) != 0 && string(prefix) != "*" {
				opt.Set(tcli.ScanOptStrictPrefix, "true")
			}
			kvs, cnt, err := client.GetTikvClient().Scan(utils.ContextWithProp(context.TODO(), opt), prefix)
			if err != nil {
				return err
			}
			for cnt > 0 {
				// write file
				if err := writeKvsToCsvFile(csvWriter, kvs); err != nil {
					return err
				}
				lastKey := utils.NextKey(kvs[len(kvs)-1].K)
				utils.Print("Write a batch, batch size:", cnt, "Last key:", kvs[len(kvs)-1].K)
				// run next batch
				kvs, cnt, err = client.GetTikvClient().Scan(utils.ContextWithProp(context.TODO(), opt), lastKey)
				if err != nil {
					return err
				}
			}
			return nil
		})
	}
}
