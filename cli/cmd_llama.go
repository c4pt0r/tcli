//go:build llama
// +build llama

package main

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strings"

	"github.com/abiosoft/ishell"
	"github.com/c4pt0r/tcli"
	"github.com/c4pt0r/tcli/query"
	"github.com/c4pt0r/tcli/utils"
	llama "github.com/go-skynet/go-llama.cpp"
)

func init() {
	RegisteredCmds = append(RegisteredCmds, AskCmd{})
}

type AskCmd struct{}

var _ tcli.Cmd = AskCmd{}

func (c AskCmd) Name() string    { return "ask" }
func (c AskCmd) Alias() []string { return []string{"ask", "a"} }
func (c AskCmd) Help() string {
	return `Ask LLama`
}

func (c AskCmd) LongHelp() string {
	s := c.Help()
	s += `
Usage:
	ask <Question>

Example:
	ask who are you?
`
	return s
}

func getQuestionString(ic *ishell.Context) string {
	ret := []string{}
	for _, arg := range ic.RawArgs[1:] {
		ret = append(ret, arg)
	}
	return strings.Join(ret, " ")
}

func (c AskCmd) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
		utils.OutputWithElapse(func() error {
			ic := utils.ExtractIshellContext(ctx)
			if len(ic.Args) < 1 {
				utils.Print(c.LongHelp())
				return nil
			}
			question := getQuestionString(ic)
			askLLama(question)
			return nil
		})
	}
}

func applyPromptTemplate(question string) string {
	tpl := "<s>[INST]\n\n%s[/INST]\n"
	return fmt.Sprintf(tpl, question)
}

func askLLama(question string) {
	l := query.LLamaModel
	if l == nil {
		fmt.Fprintf(os.Stderr, "LLama module not loaded")
		return
	}
	nthread := runtime.NumCPU()
	_, err := l.Predict(applyPromptTemplate(question), llama.SetTokenCallback(func(token string) bool {
		fmt.Print(token)
		return true
	}),
		llama.SetTokens(512),
		llama.SetThreads(nthread),
		llama.SetTopK(90),
		llama.SetTopP(0.86),
		llama.SetSeed(-1),
		llama.SetRopeFreqBase(10000.0),
		llama.SetRopeFreqScale(1.0),
	)
	fmt.Print("\n\n")
	if err != nil {
		fmt.Fprintf(os.Stderr, "LLama module predict got error: %v", err)
		return
	}
}
