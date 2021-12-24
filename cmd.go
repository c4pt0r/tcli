package tcli

import (
	"context"
	"errors"
)

type CmdSuggest struct {
	Text string
	Desc string
}

// Cmd is an abstraction of an interactable command
type Cmd interface {
	// Help is a help message
	Help() string
	// Name is the name of the command
	Name() string
	// Alias is the alias of the command
	Alias() []string
	// Suggest returns suggest list by given prefix
	Suggest(prefix string) []CmdSuggest
	// Handler is the handler of the command
	Handler(context.Context, CmdInput) Result
}

// CmdInput, [cmdName] [arg1] [arg2] [arg3]
type CmdInput interface {
	// CmdName is the first argument
	CmdName() string
	// Len is the length of args list, exclude cmdName
	Len() int
	// Arg() gets the i-th argument
	Arg(i int) []byte
	// Args() gets the full list of arguments
	Args() [][]byte
	// Get raw string
	Raw() string
}

type ResultType int

const (
	// [][]string
	ResultTypeTable ResultType = iota
	// string
	ResultTypeString
	// string
	ResultTypeError
	// bool
	ResultTypeBool
)

type Result struct {
	Tp   ResultType
	Res  interface{}
	Code int
	Err  error
}

var (
	ResultOK             = Result{Tp: ResultTypeString, Res: "OK", Code: 0}
	ResultNotImplemented = Result{Tp: ResultTypeError, Err: errors.New("not implemented"), Code: -1}
)

func ResultErr(code int, err error) Result {
	return Result{
		Tp:   ResultTypeError,
		Code: code,
		Err:  err,
		Res:  nil,
	}
}

func ResultStr(msg string) Result {
	return Result{
		Tp:   ResultTypeString,
		Code: 0,
		Err:  nil,
		Res:  msg,
	}
}

func ResultTable(rows [][]string) Result {
	return Result{
		Tp:   ResultTypeTable,
		Code: 0,
		Err:  nil,
		Res:  rows,
	}
}

type ResultOutputer interface {
	Output(Result)
}
