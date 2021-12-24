package main

import (
	"fmt"
	"os"
	"strconv"
	"tcli"
)

type Outputer interface {
	Output(r tcli.Result)
}

type StdOutputer struct{}

func (s StdOutputer) Output(r tcli.Result) {
	if r.Err != nil {
		fmt.Fprintf(os.Stderr, "Error (%d): %s\n", r.Code, r.Err)
		return
	}

	switch r.Tp {
	case tcli.ResultTypeTable:
		{
			for _, row := range r.Res.([][]string) {
				line := ""
				for _, col := range row {
					line += strconv.Quote(col) + "\t"
				}
				fmt.Println(line)
			}
			fmt.Printf("%d Rows\n", len(r.Res.([][]string)))
		}
	case tcli.ResultTypeString:
		{
			fmt.Println(strconv.Quote(r.Res.(string)))
		}
	case tcli.ResultTypeBool:
		{
			fmt.Println(r.Res.(bool))
		}
	default:
		panic("Unknown result type")
	}
}

var globalOutputer Outputer = StdOutputer{}
