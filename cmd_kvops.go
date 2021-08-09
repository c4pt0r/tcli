package main

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"

	"github.com/abiosoft/ishell"
	"github.com/magiconair/properties"
)

var (
	ScanOptKeyOnly      string = "key-only"
	ScanOptCountOnly    string = "count-only"
	ScanOptLimit        string = "limit"
	ScanOptStrictPrefix string = "strict-prefix"

	LoadFileOptBatchSize string = "batch-size"
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
func (c GetCmd) Alias() []string { return []string{"g"} }
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
			kvs := []KV{kv}
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
	var batch []KV

	batchSize := prop.GetInt(LoadFileOptBatchSize, 1000)
	for {
		rec, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		cnt++
		var key []byte
		if len(keyPrefix) > 0 {
			key = append([]byte{}, keyPrefix...)
			key = append(key, []byte(rec[0])...)
		} else {
			key = []byte(rec[0])
		}
		// TODO multi-threaded
		batch = append(batch, KV{
			K: key,
			V: []byte(rec[1]),
		})

		if len(batch) == batchSize {
			// do insert
			err := GetTikvClient().BatchPut(context.TODO(), batch)
			if err != nil {
				return err
			}
			// Show progress
			progress := rc.(*ProgressReader).GetProgress() * 100
			fmt.Printf("Progress: %d%% Count: %d Last Key: %s\n", int(progress), cnt, rec[0])
			// clean buffer
			batch = nil
		}
	}
	// may have last batch
	if len(batch) > 0 {
		// do insert
		err := GetTikvClient().BatchPut(context.TODO(), batch)
		if err != nil {
			return err
		}
	}
	fmt.Printf("Done, affected records: %d\n", cnt)
	return nil
}

func (c LoadFileCmd) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
		outputWithElapse(func() error {
			var err error
			ic := ctx.Value("ishell").(*ishell.Context)
			if len(ic.Args) == 0 {
				return errors.New(c.Help())
			}

			// set filename
			var csvFile string
			if len(ic.Args) > 0 {
				csvFile = ic.Args[0]
			}

			// set prefix
			var keyPrefix []byte
			if len(ic.Args) > 1 && !(ic.RawArgs[2] == `""` || ic.RawArgs[2] == `''`) {
				_, keyPrefix, err = getStringLit(ic.RawArgs[2])
				if err != nil {
					return err
				}
			}

			// set prop
			prop := properties.NewProperties()
			if len(ic.Args) > 2 {
				err = setOptByString(ic.RawArgs[3], prop)
				if err != nil {
					return nil
				}
			}
			// open file for read
			fp, rdr, err := openFileToProgressReader(csvFile)
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
