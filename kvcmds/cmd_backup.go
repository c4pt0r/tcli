package kvcmds

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"os"

	"github.com/c4pt0r/tcli"
	"github.com/c4pt0r/tcli/client"
	"github.com/c4pt0r/tcli/utils"
	"github.com/magiconair/properties"
)

var _ tcli.Cmd = BackupCmd{}

type BackupCmd struct{}

func (c BackupCmd) Name() string    { return "backup" }
func (c BackupCmd) Alias() []string { return []string{"backup"} }
func (c BackupCmd) Help() string {
	return "dumps kv pairs to a csv file"
}

func (c BackupCmd) LongHelp() string {
	var buf bytes.Buffer
	buf.WriteString(c.Help())
	buf.WriteString(`
Usage: 
	backup <prefix> <outfile> <opts>
Options:
	--batch-size=<size>, default 1000
Example:
	backup "t_" backup.csv --batch-size=5000
	backup * backup.csv
	backup $head  backup.csv
`)
	return buf.String()
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
				utils.Print(c.LongHelp())
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
			kvs, cnt, err := client.GetTiKVClient().Scan(utils.ContextWithProp(context.TODO(), opt), prefix)
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
				kvs, cnt, err = client.GetTiKVClient().Scan(utils.ContextWithProp(context.TODO(), opt), lastKey)
				if err != nil {
					return err
				}
			}
			return nil
		})
	}
}
