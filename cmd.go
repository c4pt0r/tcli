package tcli

import (
	"context"
)

// Cmd is an abstraction of an interactable command
type Cmd interface {
	// Help is a help message
	Help() string
	// Name is the name of the command
	Name() string
	// Alias is the alias of the command
	Alias() []string
	// Handler is the handler of the command. A *ishell.Context object named
	// `ishell` is stored in ctx
	Handler() func(ctx context.Context)
}
