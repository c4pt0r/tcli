package kvcmds

import (
	"context"
	"fmt"
	"github.com/c4pt0r/tcli/utils"
	"github.com/c4pt0r/tcli/client"
	"encoding/json"
	"strings"
)

const Json_separator = "Xuan123#@!xuan"

type PutJsonCmd struct{}

func (c PutJsonCmd) Name() string    { return "putjson" }
func (c PutJsonCmd) Alias() []string { return []string{"putjson", "putj"} }
func (c PutJsonCmd) Help() string {
	return `putjson [key] [value]`
}
func (c PutJsonCmd) LongHelp() string {
	return c.Help()
}

func (c PutJsonCmd) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
		utils.OutputWithElapse(func() error {
		    ic := utils.ExtractIshellContext(ctx)
		    // combine key and value together with the separator "Json_separator", so the length should be 1
            if len(ic.Args) != 1  {
            	fmt.Println(c.LongHelp())
            	return nil
            }

            //use split to separate k and v by Json_separator
            //now s[0] should be the key, and s[1] should be the value，with extra 'EOF' at the end
           	s := strings.Split(ic.Args[0], Json_separator)

            //value now allowed to be empty, which is just "EOF"
           	if s[1] == "EOF" {
           	    fmt.Println(c.LongHelp())
                return nil
           	}

            //take away the "EOF" from value
           	k := s[0]
            v_and_EOF := strings.Split(s[1], "EOF")
            v := v_and_EOF[0]

            //checkJson if the value is valid jsonvalue
            checkJson := json.Valid([]byte(v))

            if checkJson == true {
                fmt.Println("This is a valud json value. Successfully stored")
                err := client.GetTiKVClient().Put(context.TODO(), client.KV{K: []byte(k), V: []byte(v)})
                if err != nil {
                    fmt.Println("we have error")
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
