package main

import (
	"context"
	"flag"

	"github.com/abiosoft/ishell"
)

var (
	pdAddr         = flag.String("pd", "localhost:2379", "pd addr")
	clientLog      = flag.String("log-file", "/dev/null", "tikv client log file")
	clientLogLevel = flag.String("log-level", "info", "tikv client log level")
)

func main() {
	flag.Parse()
	initTikvClient()
	initLog()
	showWelcomeMessage()

	shell := ishell.New()
	for _, cmd := range RegisteredCmds {
		handler := cmd.Handler()
		shell.AddCmd(&ishell.Cmd{
			Name:    cmd.Name(),
			Help:    cmd.Help(),
			Aliases: cmd.Alias(),
			Func: func(c *ishell.Context) {
				handler(context.WithValue(context.TODO(), "ishell", c))
			},
		})
	}
	shell.Run()
	shell.Close()
}
