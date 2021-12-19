package main

import (
	"context"
	"os"
	"strings"

	"github.com/c-bata/go-prompt"
)

func executor(s string) {
	s = strings.TrimSpace(s)
	if s == "" {
		return
	}
	parts := strings.Split(s, " ")
	cmd := parts[0]
	if cmd == "exit" {
		os.Exit(0)
		return
	}

	c, ok := RegisteredCmdsMap[cmd]
	if ok {
		c.Handler(context.TODO(), nil)
	}
	return
}

func completer(d prompt.Document) []prompt.Suggest {
	var ret = []prompt.Suggest{}
	if d.TextBeforeCursor() == "" {
		return []prompt.Suggest{}
	}
	args := strings.Split(d.TextBeforeCursor(), " ")
	// root
	if len(args) == 1 {
		for _, c := range RegisteredCmds {
			if len(args[0]) == 1 || strings.HasPrefix(c.Name(), args[0]) {
				ret = append(ret, prompt.Suggest{
					Text: c.Name(),
				})
			}
		}
	} else {
		cmdName := args[0]
		c, ok := RegisteredCmdsMap[cmdName]
		if ok {
			suggests := c.Suggest(d.TextBeforeCursor())
			for _, s := range suggests {
				ret = append(ret, prompt.Suggest{
					Text:        s.Text,
					Description: s.Desc,
				})
			}

		}

	}
	return ret
}
