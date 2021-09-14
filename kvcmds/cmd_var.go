package kvcmds

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"tcli/utils"

	"github.com/abiosoft/ishell"
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
			let <varname>=<string value>, variable name and value are both string

			Example: scan $varname / get $varname`
}

func (c VarCmd) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
		utils.OutputWithElapse(func() error {
			ic := ctx.Value("ishell").(*ishell.Context)
			if len(ic.Args) < 1 {
				fmt.Println(c.Help())
				return errors.New("wrong args")
			}

			stmt := strings.Join(ic.RawArgs[1:], " ")
			parts := strings.Split(stmt, "=")
			if len(parts) != 2 {
				fmt.Println(c.Help())
				return errors.New("wrong format")
			}
			varName, varValue := parts[0], parts[1]
			varName = strings.TrimSpace(varName)

			if !utils.IsStringLit(varValue) {
				return errors.New("wrong format for value")
			}

			// it's a hex string literal
			value, err := utils.GetStringLit(varValue)
			if err != nil {
				return err
			}
			utils.VarSet(varName, value)
			return nil
		})
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
		utils.OutputWithElapse(func() error {
			ic := ctx.Value("ishell").(*ishell.Context)
			if len(ic.Args) < 1 {
				fmt.Println(c.Help())
				return errors.New("wrong args number")
			}

			varName := ic.Args[0]
			if !strings.HasPrefix(varName, "$") {
				return errors.New("varname should have $ as prefix")
			}
			varName = varName[1:]
			if val, ok := utils.VarGet(varName); ok {
				fmt.Printf("string:\"%s\" bytes: %v\n", val, val)
			} else {
				return errors.New("no such variable")
			}
			return nil
		})
	}
}

type PrintVarsCmd struct{}

func (c PrintVarsCmd) Name() string    { return "env" }
func (c PrintVarsCmd) Alias() []string { return []string{"env"} }
func (c PrintVarsCmd) Help() string {
	return `print env variables`
}

func (c PrintVarsCmd) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
		utils.OutputWithElapse(func() error {
			utils.PrintGlobalVaribles()
			return nil
		})
	}
}

type PrintSysVarsCmd struct{}

func (c PrintSysVarsCmd) Name() string    { return "sysenv" }
func (c PrintSysVarsCmd) Alias() []string { return []string{"sysenv"} }
func (c PrintSysVarsCmd) Help() string {
	return `print system env variables`
}

func (c PrintSysVarsCmd) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
		utils.OutputWithElapse(func() error {
			utils.PrintSysVaribles()
			return nil
		})
	}
}

type SetSysVarsCmd struct{}

func (c SetSysVarsCmd) Name() string    { return "setsysenv" }
func (c SetSysVarsCmd) Alias() []string { return []string{"setsysenv"} }
func (c SetSysVarsCmd) Help() string {
	return `set system env variables, setsysenv [key] [value]`
}

func (c SetSysVarsCmd) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
		utils.OutputWithElapse(func() error {
			ic := ctx.Value("ishell").(*ishell.Context)
			if len(ic.Args) < 1 {
				fmt.Println(c.Help())
				return errors.New("wrong args number")
			}

			varName := ic.Args[0]
			if !strings.HasPrefix(varName, "$") {
				return errors.New("varname should have $ as prefix")
			}
			varName = varName[1:]
			if val, ok := utils.VarGet(varName); ok {
				fmt.Printf("string:\"%s\" bytes: %v\n", val, val)
			} else {
				return errors.New("no such variable")
			}
			return nil

		})
	}
}
