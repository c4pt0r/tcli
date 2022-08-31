package kvcmds

import (
	"context"
	"github.com/c4pt0r/tcli/utils"
	"github.com/c4pt0r/tcli/client"
	"fmt"
    "encoding/json"
	"github.com/oliveagle/jsonpath"
)

type GetJsonCmd struct{}

func (c GetJsonCmd) Name() string    { return "getjson" }
func (c GetJsonCmd) Alias() []string { return []string{"getj", } }
func (c GetJsonCmd) Help() string {
	return `getjson [key] [jsonpath]`
}

func (c GetJsonCmd) LongHelp() string {
	return c.Help()
}

func (c GetJsonCmd) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
		utils.OutputWithElapse(func() error {
			ic := utils.ExtractIshellContext(ctx)
			if len(ic.Args) != 2 {
				utils.Print(c.Help())
				return nil
			}

			s := ic.RawArgs[1]
			// it's a hex string literal
			k, err := utils.GetStringLit(s)
			if err != nil {
				return err
			}

            kv, err := client.GetTiKVClient().Get(context.TODO(), client.Key(k))
			if err != nil {
				return err
			}
			//json_rawdata stores the jsonvalue of the key
			json_rawdata := string(kv.V)

            //json path starts with "$.", but there is no "$." from the customer input. Add on
            edited_jsonpath := "$." + ic.RawArgs[2]

            var json_data interface{}

            //change the type of json_rawdata(string) to the type available for jsonpath, and store it to json_data
            json.Unmarshal([]byte(json_rawdata), &json_data)

            //find the jsonpath in the json_data and print
            ans, err := jsonpath.JsonPathLookup(json_data, edited_jsonpath)
            if err == nil {
                fmt.Println(ans)
            } else {
                fmt.Println(err)
            }

			return nil
		})
	}
}
