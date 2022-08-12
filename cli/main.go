package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"tcli"
	"tcli/client"
	"tcli/kvcmds"
	"tcli/opcmds"
	"tcli/utils"

	"github.com/abiosoft/ishell"
	"github.com/c4pt0r/log"
	"github.com/fatih/color"
	plog "github.com/pingcap/log"
)

const Json_separator = "xuanxuan"
//

var (
	pdAddr         = flag.String("pd", "localhost:2379", "PD addr")
	clientLog      = flag.String("log-file", "/dev/null", "TiKV client log file")
	clientLogLevel = flag.String("log-level", "info", "TiKV client log level")
	clientmode     = flag.String("mode", "txn", "TiKV API mode, accepted values: raw/txn")
)
var (
	logo string = "                    /           \n" +
		"                %%#######%%               \n" +
		"           .#################             \n" +
		"       ##########################*        \n" +
		"   #############        *############%%   \n" +
		"(###########           ###############    \n" +
		"(######(             ###################     %s\n" +
		"(######             (#########    ######  \n" +
		"(######     #%%      (######       ######  \n" +
		"(###### %%####%%      (######       ######     https://tikv.org\n" +
		"(############%%      (######       ######     https://pingcap.com\n" +
		"(############%%      (######       ###### \n" +
		"(############%%      (######       ###### \n" +
		"(############%%      (######   .######### \n" +
		" #############,     (##################(  \n" +
		"     /############# (##############.      \n" +
		"          ####################%%          \n" +
		"              %%###########(              \n" +
		"                  /###,                   \n"
)

// RegisteredCmds global command registration
// the Cmd objects inside this array can only be used
var RegisteredCmds = []tcli.Cmd{
	kvcmds.ScanCmd{},
	kvcmds.ScanPrefixCmd{},
	kvcmds.HeadCmd{},
	kvcmds.PutCmd{},
	// add
	kvcmds.PutJsonCmd{},
	kvcmds.BackupCmd{},
	kvcmds.NewBenchCmd(
		kvcmds.NewYcsbBench(*pdAddr),
	),
	kvcmds.GetCmd{},
	//add
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

	opcmds.ListStoresCmd{},
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
	fmt.Printf(
		"%s, Welcome, TiKV Cluster ID: %s, TiKV Mode: %s",
		logo,
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
		log.I("pd instance info:", member)
	}
	stores, err := client.GetTiKVClient().GetStores()
	if err != nil {
		log.F(err)
	}
	for _, store := range stores {
		log.I("tikv instance info:", store)
	}
}

func main() {
	flag.Parse()
	initLog()
	if err := client.InitTiKVClient([]string{*pdAddr}, *clientmode); err != nil {
		log.Fatal(err)
	}
	utils.InitBuiltinVaribles()
	showWelcomeMessage()

	// set shell prompts
	shell := ishell.New()
	if client.GetTiKVClient().GetClientMode() == client.RAW_CLIENT {
		// TODO: add pd leader addr after we can get PD client from RawKV client.
		shell.SetPrompt(fmt.Sprintf("%s> ", client.GetTiKVClient().GetClientMode()))
	} else {
		pdLeaderAddr := client.GetTiKVClient().GetPDClient().GetLeaderAddr()
		shell.SetPrompt(fmt.Sprintf("%s(%s)> ", client.GetTiKVClient().GetClientMode(), pdLeaderAddr))
	}

	// register shell commands
	for i, cmd := range RegisteredCmds {
	    // 当i=4的时候为putjson 的cmd
	    if i == 4 {
            handler := cmd.Handler()
    		shell.AddCmd(&ishell.Cmd{
    			Name:    cmd.Name(),
    			Help:    cmd.Help(),
    			Aliases: cmd.Alias(),
    			Func: func(c *ishell.Context) {
    			    /*
    			    先输入一个参数作为key，输入换行，输出"Please enter a json value end with 'EOF'."
    			    接下来输入value，也就是一个json value。

    			    我的逻辑是将key和value通过Json_separator链接起来，成为一个string，再放到handler去处理。
    			    Json_separator应当是一个客户不会在key或者value中用到的特殊值。

    			    如果结尾的'EOF'是一个可能会被存储到json value中的内容，那么可以进行更换。Json_separator也是同理

    			    */
    			    c.ShowPrompt(false)
                    defer c.ShowPrompt(true)
                    c.Println("Please enter a json value end with 'EOF'.")
                    lines := c.ReadMultiLines("EOF")
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
	        handler := cmd.Handler()
    		shell.AddCmd(&ishell.Cmd{
    			Name:    cmd.Name(),
    			Help:    cmd.Help(),
    			Aliases: cmd.Alias(),
    			Func: func(c *ishell.Context) {
    				ctx := context.WithValue(context.TODO(), "ishell", c)
    				fmt.Println(color.WhiteString("Input:"), strings.Join(c.RawArgs, " "))
    				handler(ctx)
    			},
    		})
	    }

	}


	shell.Run()
	shell.Close()
}
