package kvcmds

import (
	"context"
	"tcli/client"
	"tcli/utils"
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

func (c GetJsonCmd) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
		utils.OutputWithElapse(func() error {
			ic := utils.ExtractIshellContext(ctx)
			// 更改小于号到不等号逻辑参考cmd_put.go注释
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
			//json_rawdata储存的是key所对应的jsonvalue
			json_rawdata := string(kv.V)

            //json path是以"$."开头的，客户输入的时候不需要输入这个，进行填补
            edited_jsonpath := "$." + ic.RawArgs[2]

            var json_data interface{}

            //将string类型的json_rawdata转换成可以被jsonpath扫描的类型，存储到json_data
            json.Unmarshal([]byte(json_rawdata), &json_data)

            //在json_data中寻找客户输入的，增加了"$."之后的edited_jsonpath
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
