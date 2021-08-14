package opcmds

import "context"

type PingCmd struct{}

func (c PingCmd) Name() string    { return ".ping" }
func (c PingCmd) Alias() []string { return []string{".p"} }
func (c PingCmd) Help() string {
	return "ping pd, usage: [.ping|.p] 192.168.1.1:2379"
}

func (c PingCmd) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
	}
}
