package kvcmds

import (
	"context"
	"tcli"

	"github.com/c4pt0r/log"
)

type ScanCmd struct{}

func (c ScanCmd) Name() string    { return "scan" }
func (c ScanCmd) Alias() []string { return []string{"scan"} }
func (c ScanCmd) Help() string {
	return `scan`
}

func (c ScanCmd) Suggest(line string) []tcli.CmdSuggest {
	return []tcli.CmdSuggest{}
}

func (c ScanCmd) Handler(ctx context.Context, input tcli.CmdInput) tcli.Result {
	return tcli.ResultOK
}

type ScanPrefixCmd struct{}

func (c ScanPrefixCmd) Name() string    { return "scanp" }
func (c ScanPrefixCmd) Alias() []string { return []string{"scanp"} }
func (c ScanPrefixCmd) Help() string {
	return `scan-prefix`
}

func (c ScanPrefixCmd) Suggest(line string) []tcli.CmdSuggest {
	return []tcli.CmdSuggest{}
}

func (c ScanPrefixCmd) Handler(ctx context.Context, input tcli.CmdInput) tcli.Result {
	log.D("scan handler")
	return tcli.ResultOK
}

type HeadCmd struct{}

func (c HeadCmd) Name() string    { return "head" }
func (c HeadCmd) Alias() []string { return []string{"head"} }
func (c HeadCmd) Help() string {
	return `head`
}
func (c HeadCmd) Suggest(line string) []tcli.CmdSuggest {
	return []tcli.CmdSuggest{}
}
func (c HeadCmd) Handler(ctx context.Context, input tcli.CmdInput) tcli.Result {
	log.D("head handler")
	return tcli.ResultOK
}
