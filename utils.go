package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"

	"log"

	"github.com/fatih/color"
	"github.com/magiconair/properties"
	"github.com/olekukonko/tablewriter"
	plog "github.com/pingcap/log"
	"go.uber.org/atomic"
)

var (
	logo string = `                   /                   
                %#######%               
           .#################           
       ##########################*      
   #############        *############%  
(###########             ###############
(######(             ###################
(######             (#########    ######
(######     #%      (######       ######
(###### %####%      (######       ######
(############%      (######       ######
(############%      (######       ######
(############%      (######       ######
(############%      (######   .#########
 #############,     (##################(
     /############# (##############.    
          ####################%         
              %###########(             
                  /###,   
`
)

func initLog() {
	// keep pingcap's log silent
	conf := &plog.Config{Level: *clientLogLevel, File: plog.FileLogConfig{Filename: *clientLog}}
	lg, r, _ := plog.InitLogger(conf)
	plog.ReplaceGlobals(lg, r)
}

func showWelcomeMessage() {
	color.Red(logo)
	pdClient := GetTikvClient().GetPDClient()
	// show pd members
	members, err := pdClient.GetAllMembers(context.TODO())
	if err != nil {
		log.Fatalf("%v", err)
	}
	fmt.Println("PD Peers:")
	for _, member := range members {
		fmt.Println(member)
	}
	color.Green("Welcome, TiKV Cluster ID: %d", pdClient.GetClusterID(context.TODO()))

}

func printTable(data [][]string) {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(data[0])
	table.SetBorders(tablewriter.Border{Left: true, Top: true, Right: true, Bottom: true})
	table.SetCenterSeparator("|")
	table.AppendBulk(data[1:])
	table.Render()
}

func outputWithElapse(f func() error) error {
	tt := time.Now()
	err := f()
	if err != nil {
		fmt.Printf("Error: %s, Elapse: %d ms\n", err, time.Since(tt)/time.Millisecond)
	} else {
		fmt.Printf("Success, Elapse: %d ms\n", time.Since(tt)/time.Millisecond)
	}
	return err
}

func hexstr2bytes(hexStr string) ([]byte, error) {
	return hex.DecodeString(hexStr)
}

func bytes2hex(s []byte) string {
	return hex.EncodeToString(s)
}

// String Literal Parsing
// h'12332321' <---- Hex string
type StrLitType int

const (
	StrLitHex StrLitType = iota
	StrLitNormal
)

var (
	_reHexStr, _reNormalStr *regexp.Regexp
)

func init() {
	_reHexStr, _ = regexp.Compile(`h"([^"\\]|\\[\s\S])*"|h'([^'\\]|\\[\s\S])*'`)
	_reNormalStr, _ = regexp.Compile(`"([^"\\]|\\[\s\S])*"|'([^'\\]|\\[\s\S])*'`)
}

func getStringLit(raw string) (StrLitType, []byte, error) {
	// h"" | h''
	if _reHexStr.MatchString(raw) {
		out := _reHexStr.FindString(raw)
		val := string(out[2 : len(out)-1])
		b, err := hexstr2bytes(val)
		if err != nil {
			return StrLitNormal, nil, err
		}
		return StrLitHex, b, nil
	}
	// "" | ''
	if _reNormalStr.MatchString(raw) {
		out := _reNormalStr.FindString(raw)
		val := out[1 : len(out)-1]
		return StrLitNormal, []byte(val), nil
	}
	return StrLitNormal, []byte(raw), nil
}

func setOptByString(ss []string, props *properties.Properties) error {
	// hack
	var items []string
	var kvItems []string
	var boolItems []string
	for _, item := range ss {
		kvs := strings.Split(item, ",")
		items = append(items, kvs...)
	}
	// 1. ss:  opt1=val1 opt2=val2,    opt3=val3 => opt1=val1\nopt2=val2\n
	for _, item := range items {
		kv := strings.ReplaceAll(item, " ", "\n")
		// item like: key-only,count-only
		if strings.Contains(kv, "=") {
			kvItems = append(kvItems, kv)
		} else {
			boolItems = append(boolItems, kv)
		}
	}
	confBuf := strings.Join(kvItems, "\n")
	props.Load([]byte(confBuf), properties.UTF8)

	for _, item := range boolItems {
		props.Set(item, "true")
	}

	fmt.Println(ss, boolItems, props.String())

	return nil
}

func contextWithProp(ctx context.Context, p *properties.Properties) context.Context {
	return context.WithValue(ctx, propertiesKey, p)
}

func propFromContext(ctx context.Context) *properties.Properties {
	prop := ctx.Value(propertiesKey).(*properties.Properties)
	if prop == nil {
		return properties.NewProperties()
	}
	return prop
}

type ProgressReader struct {
	totalSz int64
	readSz  *atomic.Int32
	rdr     io.Reader
	err     atomic.Value
}

func NewProgressReader(r io.Reader, total int64) *ProgressReader {
	return &ProgressReader{
		totalSz: total,
		readSz:  atomic.NewInt32(0),
		err:     atomic.Value{},
		rdr:     r,
	}
}

func (pr *ProgressReader) Read(b []byte) (int, error) {
	n, err := pr.rdr.Read(b)
	if err != nil {
		pr.err.Store(err)
		return n, err
	}
	pr.readSz.Add(int32(n))
	return n, err
}

func (pr *ProgressReader) GetProgress() float64 {
	return float64(pr.readSz.Load()) / float64(pr.totalSz)
}

func (pr *ProgressReader) Error() error {
	v := pr.err.Load()
	if v != nil {
		return v.(error)
	}
	return nil
}

func openFileToProgressReader(fname string) (*os.File, *ProgressReader, error) {
	fp, err := os.Open(fname)
	if err != nil {
		return nil, nil, err
	}
	fi, err := fp.Stat()
	if err != nil {
		return nil, nil, err
	}
	pr := NewProgressReader(fp, fi.Size())
	return fp, pr, nil
}
