package tcli

import (
	"context"
)

// Cmd is an abstraction of an interactable command
type Cmd interface {
	// Help is a help message
	Help() string
	// LongHelp is a long help message
	LongHelp() string
	// Name is the name of the command
	Name() string
	// Alias is the alias of the command
	Alias() []string
	// Handler is the handler of the command
	// `ishell` is stored in ctx
	Handler() func(ctx context.Context)
	// Completer
	// Completer() func(ctx context.Context, args []string) []string
}
