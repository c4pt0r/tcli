package kvcmds

import (
	"context"
)

type VarCmd struct {
}

func NewVarCmd() VarCmd {
	return VarCmd{}
}

func (c VarCmd) Name() string    { return "var" }
func (c VarCmd) Alias() []string { return []string{"var"} }
func (c VarCmd) Help() string {
	return `set variable, usage:
			var <varname>=<string value>, variable name and value are both string

			variable usage, example: scan $varname / get $varname`
}

func (c VarCmd) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
	}
}

type EchoCmd struct{}

func (c EchoCmd) Name() string    { return "echo" }
func (c EchoCmd) Alias() []string { return []string{"echo"} }
func (c EchoCmd) Help() string {
	return `echo $<varname>`
}

func (c EchoCmd) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
	}
}
