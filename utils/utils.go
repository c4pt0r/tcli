package utils

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

	"github.com/abiosoft/ishell"
	"github.com/magiconair/properties"
	"github.com/manifoldco/promptui"
	"github.com/olekukonko/tablewriter"
	"go.uber.org/atomic"
)

var (
	propertiesKey = "property"
)

func PrintTable(data [][]string) {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(data[0])
	table.SetBorders(tablewriter.Border{Left: true, Top: true, Right: true, Bottom: true})
	table.SetCenterSeparator("|")
	table.AppendBulk(data[1:])
	table.Render()
}

func Hexstr2bytes(hexStr string) ([]byte, error) {
	return hex.DecodeString(hexStr)
}

func Bytes2hex(s []byte) string {
	return hex.EncodeToString(s)
}

// String Literal Parsing
// h'12332321' <---- Hex string
type StrLitType int

const (
	StrLitHex StrLitType = iota
	StrLitNormal
)

func Bytes2StrLit(b []byte) string {
	return fmt.Sprintf("h'%s'", Bytes2hex(b))
}

var (
	_reHexStr, _reNormalStr *regexp.Regexp
)

func init() {
	_reHexStr, _ = regexp.Compile(`h"([^"\\]|\\[\s\S])*"|h'([^'\\]|\\[\s\S])*'`)
	_reNormalStr, _ = regexp.Compile(`"([^"\\]|\\[\s\S])*"|'([^'\\]|\\[\s\S])*'`)
}

func IsStringLit(raw string) bool {
	return _reHexStr.MatchString(raw) || _reNormalStr.MatchString(raw)
}

func GetStringLit(raw string) ([]byte, error) {
	if raw[0] == '$' {
		varVal, ok := VarGet(raw[1:])
		if !ok {
			return nil, errors.New("no such variable")
		}
		return varVal, nil
	}
	// h"" | h''
	if _reHexStr.MatchString(raw) {
		out := _reHexStr.FindString(raw)
		val := string(out[2 : len(out)-1])
		b, err := Hexstr2bytes(val)
		if err != nil {
			return nil, err
		}
		return b, nil
	}
	// "" | ''
	if _reNormalStr.MatchString(raw) {
		out := _reNormalStr.FindString(raw)
		val := out[1 : len(out)-1]
		return []byte(val), nil
	}
	return []byte(raw), nil
}

func SetOptByString(ss []string, props *properties.Properties) error {
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
	return nil
}

func ContextWithProp(ctx context.Context, p *properties.Properties) context.Context {
	return context.WithValue(ctx, propertiesKey, p)
}

func PropFromContext(ctx context.Context) *properties.Properties {
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

func OpenFileToProgressReader(fname string) (*os.File, *ProgressReader, error) {
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

// 1 yes, 0 no, -1 return
func AskYesNo(msg string, def string) int {
	prompt := promptui.Select{
		Label: msg,
		Items: []string{"yes", "no"},
	}

	// TODO
	switch def {
	case "yes":
	case "no":
	}

	_, res, err := prompt.Run()
	if err != nil {
		return -1
	}

	switch res {
	case "yes":
		return 1
	case "no":
		return 0
	}
	return -1
}

func Print(a ...interface{}) {
	fmt.Println(a...)
}

func ExtractIshellContext(ctx context.Context) *ishell.Context {
	ic := ctx.Value("ishell").(*ishell.Context)
	return ic
}

// NextKey returns the next key in byte-order.
func NextKey(k []byte) []byte {
	// add 0x0 to the end of key
	buf := make([]byte, len(k)+1)
	copy(buf, k)
	return buf
}
