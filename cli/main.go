package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/c4pt0r/tcli"
	"github.com/c4pt0r/tcli/client"
	"github.com/c4pt0r/tcli/kvcmds"
	"github.com/c4pt0r/tcli/opcmds"
	"github.com/c4pt0r/tcli/utils"

	"github.com/abiosoft/ishell"
	"github.com/c4pt0r/log"
	"github.com/fatih/color"
	plog "github.com/pingcap/log"
)

const Json_separator = "Xuan123#@!xuan"

var (
	pdAddr         = flag.String("pd", "localhost:2379", "PD addr")
	clientLog      = flag.String("log-file", "/dev/null", "TiKV client log file")
	clientLogLevel = flag.String("log-level", "info", "TiKV client log level")
	clientmode     = flag.String("mode", "txn", "TiKV API mode, accepted values: [raw | txn]")
	resultFmt      = flag.String("output-format", "table", "output format, accepted values: [table | json]")
)
var (
	logo string = ""
)

// RegisteredCmds global command registration
// the Cmd objects inside this array can only be used
var RegisteredCmds = []tcli.Cmd{
	kvcmds.ScanCmd{},
	kvcmds.ScanPrefixCmd{},
	kvcmds.HeadCmd{},
	kvcmds.PutCmd{},
	kvcmds.PutJsonCmd{},
	kvcmds.BackupCmd{},
	kvcmds.NewBenchCmd(
		kvcmds.NewYcsbBench(*pdAddr),
	),
	kvcmds.GetCmd{},
	kvcmds.GetJsonCmd{},
	kvcmds.LoadCsvCmd{},
	kvcmds.DeleteCmd{},
	kvcmds.DeletePrefixCmd{},
	kvcmds.DeleteAllCmd{},
	kvcmds.CountCmd{},
	kvcmds.EchoCmd{},
	kvcmds.HexCmd{},
	kvcmds.VarCmd{},
	kvcmds.PrintVarsCmd{},
	kvcmds.PrintSysVarsCmd{},
	kvcmds.SysVarCmd{},
	opcmds.ListStoresCmd{},
	opcmds.ListPDCmd{},
	//opcmds.ConnectCmd{},
	//opcmds.ConfigEditorCmd{},
}

func initLog() {
	// keep pingcap's log silent
	conf := &plog.Config{Level: *clientLogLevel, File: plog.FileLogConfig{Filename: *clientLog}}
	lg, r, _ := plog.InitLogger(conf)
	plog.ReplaceGlobals(lg, r)

	log.SetLevelByString(*clientLogLevel)
}

func showWelcomeMessage() {
	fmt.Fprintf(
		os.Stderr,
		"Welcome, TiKV Cluster ID: %s, TiKV Mode: %s\n",
		client.GetTiKVClient().GetClusterID(),
		client.GetTiKVClient().GetClientMode(),
	)

	if client.GetTiKVClient().GetClientMode() == client.RAW_CLIENT {
		return
	}

	// show pd members
	pdClient := client.GetTiKVClient().GetPDClient()
	members, err := pdClient.GetAllMembers(context.TODO())
	if err != nil {
		log.F(err)
	}
	for _, member := range members {
		log.D("pd instance info:", member)
	}
	stores, err := client.GetTiKVClient().GetStores()
	if err != nil {
		log.F(err)
	}
	for _, store := range stores {
		log.D("tikv instance info:", store)
	}
}

func main() {
	flag.Parse()
	initLog()
	fmt.Fprintf(os.Stderr, "Try connecting to PD: %s...", *pdAddr)
	if err := client.InitTiKVClient([]string{*pdAddr}, *clientmode); err != nil {
		log.Fatal(err)
	}
	fmt.Fprintf(os.Stderr, "done\n")
	utils.InitBuiltinVaribles()

	// Set output format
	utils.SysVarSet(utils.SysVarPrintFormatKey, *resultFmt)

	showWelcomeMessage()

	// set shell prompts
	shell := ishell.New()
	if client.GetTiKVClient().GetClientMode() == client.RAW_CLIENT {
		// TODO: add pd leader addr after we can get PD client from RawKV client.
		shell.SetPrompt(fmt.Sprintf("%s> ", client.GetTiKVClient().GetClientMode()))
	} else {
		pdLeaderAddr := client.GetTiKVClient().GetPDClient().GetLeaderAddr()
		shell.SetPrompt(fmt.Sprintf("%s @ %s> ", client.GetTiKVClient().GetClientMode(), pdLeaderAddr))
	}
	shell.EOF(func(c *ishell.Context) { shell.Close() })
	shell.AutoHelp(false)

	// register shell commands
	for i, cmd := range RegisteredCmds {
		handler := cmd.Handler()
		longhelp := cmd.LongHelp()
        // putjson is when i == 4. We need a multi-line input
        if i == 4 {
    		shell.AddCmd(&ishell.Cmd{
    			Name:    cmd.Name(),
    			Help:    cmd.Help(),
    			LongHelp: cmd.LongHelp(),
    			Aliases: cmd.Alias(),
    			Func: func(c *ishell.Context) {
    			    c.ShowPrompt(false)
                    defer c.ShowPrompt(true)
                    /* first type in a key, then enter.
                     screen print "Please enter a json value end with 'EOF'."
                     now input the multi-line json value. It has to end with "EOF".*/
                    c.Println("Please enter a json value end with 'EOF'.")
                    lines := c.ReadMultiLines("EOF")
                    /* connect the key and value with Json_separator to one long string.
                       the value of Json_separator and "EOF" should be strings that will not exist in the customer input.
                       separate the combined string later on by Json_separator and get the key and multi-line jsonvalue
                    */
                    if len(c.Args) == 1 {
                        c.Args[0] = c.Args[0] + Json_separator + lines
                        c.RawArgs[1] = c.RawArgs[1] + " " + lines
                    }
    				ctx := context.WithValue(context.TODO(), "ishell", c)
    				fmt.Println(color.WhiteString("Input:"), strings.Join(c.RawArgs, " "))
    				fmt.Print("\n")
    				handler(ctx)
    			},
    		})
	    } else {
    		shell.AddCmd(&ishell.Cmd{
    			Name:     cmd.Name(),
    			Help:     cmd.Help(),
    			LongHelp: cmd.LongHelp(),
    			Aliases:  cmd.Alias(),
    			Func: func(c *ishell.Context) {
    				ctx := context.WithValue(context.TODO(), "ishell", c)
    				if strings.ToLower(*clientLogLevel) == "debug" {
    					fmt.Fprintln(os.Stderr, color.YellowString("Input:"), c.RawArgs)
    					for _, arg := range c.Args {
    						fmt.Fprintln(os.Stderr, color.YellowString("Arg:"), arg)
    					}
    					fmt.Fprintf(os.Stderr, "\033[33mOutput:\033[0m\n")
    				}
    				if len(c.Args) > 0 && c.Args[0] == "--help" {
    					c.Println(longhelp)
    					return
    				}
    				handler(ctx)
    			},
    		})

	    }
	}
	shell.Run()
	shell.Close()
}
