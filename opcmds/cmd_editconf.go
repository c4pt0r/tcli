package opcmds

import (
	"context"

	"github.com/AlecAivazis/survey/v2"
	"github.com/c4pt0r/tcli/utils"
)

// not implemented yet

type ConfigEditorCmd struct{}

var _ ConfigEditorCmd = ConfigEditorCmd{}

func (c ConfigEditorCmd) Name() string    { return ".config" }
func (c ConfigEditorCmd) Alias() []string { return []string{".config"} }
func (c ConfigEditorCmd) Help() string {
	return "edit tikv config"
}

func (c ConfigEditorCmd) LongHelp() string {
	return c.Help()
}

func (c ConfigEditorCmd) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
		prompt := &survey.Editor{
			Message:       "Edit TiKV Config File",
			Default:       "TODO",
			HideDefault:   true,
			AppendDefault: true,
		}
		var content string
		survey.AskOne(prompt, &content)
		// TODO
		utils.Print(content)
	}
}
