package kvcmds

import (
	"context"
	"fmt"
	"tcli/client"
	"tcli/utils"
	"encoding/json"
	"strings"
)

const Json_separator = "xuanxuan"

type PutJsonCmd struct{}

func (c PutJsonCmd) Name() string    { return "putjson" }
func (c PutJsonCmd) Alias() []string { return []string{"putjson", "putj"} }
func (c PutJsonCmd) Help() string {
	return `putjson [key] [value]`
}

func (c PutJsonCmd) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
		utils.OutputWithElapse(func() error {
		    ic := utils.ExtractIshellContext(ctx)
		    // 因为我在main中将key和value合并到了一起，所以这里args的长度应当为1
            if len(ic.Args) != 1  {
            	fmt.Println(c.Help())
            	return nil
            }

            //用split把kv拆开，Json_separator为关键字
            //此时s[0]应当为key s[1]应当为value，但是结尾多出了一个'EOF'
           	s := strings.Split(ic.Args[0], Json_separator)

            //判断，value不能为空，即只有EOF
           	if s[1] == "EOF" {
           	    fmt.Println(c.Help())
                return nil
           	}

            //结尾处是有一个不属于json value的'EOF'在的，要把这个拆分开
           	k := s[0]
            v_and_EOF := strings.Split(s[1], "EOF")
            v := v_and_EOF[0]

            //checkJson用来判断value是否为一个合法的jsonvalue
            checkJson := json.Valid([]byte(v))

            if checkJson == true {
                fmt.Println("This is a valud json value. Successfully stored")
                err := client.GetTiKVClient().Put(context.TODO(), client.KV{K: []byte(k), V: []byte(v)})
                if err != nil {
                	return err
                }
                return nil
            } else {
                fmt.Println("This is an invalud json value. Please try again")
                return nil
            }

           	return nil
		})
	}
}
