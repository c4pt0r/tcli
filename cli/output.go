package main

import (
	"fmt"
	"os"
	"tcli"
	"tcli/utils"
)

type StdOutputer struct{}

func (s StdOutputer) Output(r tcli.Result) {
	if r.Err != nil {
		fmt.Fprintf(os.Stderr, "Error (%d): %s", r.Code, r.Err)
		return
	}

	switch r.Tp {
	case tcli.ResultTypeTable:
		{
			utils.PrintTable(r.Res.([][]string))
			fmt.Printf("%d Rows\n", len(r.Res.([][]string)))
		}
	case tcli.ResultTypeString:
		{
			fmt.Println(r.Res.(string))
		}
	case tcli.ResultTypeBool:
		{
			fmt.Println(r.Res.(bool))
		}
	default:
		panic("Unknown result type")
	}
}
