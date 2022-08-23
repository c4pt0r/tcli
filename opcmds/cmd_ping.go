package opcmds

import (
	"context"

	"github.com/c4pt0r/tcli"
)

type PingCmd struct{}

var _ tcli.Cmd = PingCmd{}

func (c PingCmd) Name() string    { return ".ping" }
func (c PingCmd) Alias() []string { return []string{".p"} }
func (c PingCmd) Help() string {
	return "ping pd, usage: [.ping|.p] 192.168.1.1:2379"
}

func (c PingCmd) LongHelp() string {
	return c.Help()
}

func (c PingCmd) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
	}
}
