package opcmds

import (
	"context"
	"fmt"

	"github.com/abiosoft/ishell"
)

type ConnectCmd struct{}

func (c ConnectCmd) Name() string    { return ".connect" }
func (c ConnectCmd) Alias() []string { return []string{".c", ".conn"} }
func (c ConnectCmd) Help() string {
	return "connect to a tikv cluster, usage: [.connect|.conn|.c] [pd addr], example: .c 192.168.1.1:2379"
}

func (c ConnectCmd) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
		ic := ctx.Value("ishell").(*ishell.Context)
		if len(ic.Args) > 0 {
			ic.Println("connecting to", ic.Args[0])
			ic.SetPrompt(fmt.Sprintf("[%s] >>> ", ic.Args[0]))
		} else {
			ic.Println(ic.Cmd.HelpText())
		}
	}
}
