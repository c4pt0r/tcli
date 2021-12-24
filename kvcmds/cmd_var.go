package kvcmds

import (
	"context"
	"errors"
	"tcli"
	"tcli/utils"

	"github.com/c4pt0r/log"
)

type VarCmd struct {
}

func NewVarCmd() VarCmd {
	return VarCmd{}
}

func (c VarCmd) Name() string    { return "var" }
func (c VarCmd) Alias() []string { return []string{"var", "let"} }
func (c VarCmd) Help() string {
	return `set variables, usage:
			    var <varname>=<string value>, variable name and value are both string
				  example: scan $varname or get $varname`
}

func (c VarCmd) Handler(ctx context.Context, input tcli.CmdInput) tcli.Result {
	log.D("var handler")
	return tcli.ResultOK
}

type EchoCmd struct{}

func (c EchoCmd) Suggest(prefix string) []tcli.CmdSuggest {
	return []tcli.CmdSuggest{}
}
func (c EchoCmd) Name() string    { return "echo" }
func (c EchoCmd) Alias() []string { return []string{"echo"} }
func (c EchoCmd) Help() string {
	return `echo <arg>`
}

func (c EchoCmd) Handler(ctx context.Context, input tcli.CmdInput) tcli.Result {
	content := input.Arg(1)
	if content[0] == '$' {
		v, ok := utils.GlobalEnv().Get(string(content[1:]))
		if ok {
			return tcli.ResultStr(string(v))
		} else {
			return tcli.ResultErr(400, errors.New("varible not found"))
		}
	}
	return tcli.ResultStr(string(content))
}

type PrintVarsCmd struct{}

func (c PrintVarsCmd) Name() string    { return "env" }
func (c PrintVarsCmd) Alias() []string { return []string{"env"} }
func (c PrintVarsCmd) Help() string {
	return `print env variables`
}

func (c PrintVarsCmd) Handler(ctx context.Context, input tcli.CmdInput) tcli.Result {
	return tcli.ResultOK
}

type PrintSysVarsCmd struct{}

func (c PrintSysVarsCmd) Name() string    { return "sysenv" }
func (c PrintSysVarsCmd) Alias() []string { return []string{"sysenv"} }
func (c PrintSysVarsCmd) Help() string {
	return `print system env variables`
}

func (c PrintSysVarsCmd) Handler(ctx context.Context, input tcli.CmdInput) tcli.Result {
	return tcli.ResultOK
}

type SetSysVarsCmd struct{}

func (c SetSysVarsCmd) Name() string    { return "setsysenv" }
func (c SetSysVarsCmd) Alias() []string { return []string{"setsysenv"} }
func (c SetSysVarsCmd) Help() string {
	return `set system env variables, setsysenv [key] [value]`
}

func (c SetSysVarsCmd) Handler(ctx context.Context, input tcli.CmdInput) tcli.Result {
	log.D("setsysvar handler")
	return tcli.ResultOK
}

type HexCmd struct{}

func (c HexCmd) Name() string    { return "hexdump" }
func (c HexCmd) Alias() []string { return []string{"hex"} }
func (c HexCmd) Help() string {
	return `hexdump <string>`
}

func (c HexCmd) Handler(ctx context.Context, input tcli.CmdInput) tcli.Result {
	log.D("hex handler")
	return tcli.ResultOK
}
