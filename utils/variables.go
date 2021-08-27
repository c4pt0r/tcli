package utils

import (
	"fmt"
	"strings"
	"sync"
)

var (
	_globalVariables = make(map[string][]byte)
	_globalVarMutex  sync.RWMutex

	_builtinVars = [][]string{
		[]string{`head`, "\x00"},
	}
)

func VarGet(varname string) ([]byte, bool) {
	_globalVarMutex.RLock()
	defer _globalVarMutex.RUnlock()
	val, ok := _globalVariables[varname]
	return val, ok
}

func VarSet(varname string, val []byte) {
	_globalVarMutex.Lock()
	defer _globalVarMutex.Unlock()
	_globalVariables[varname] = append([]byte{}, val...)
}

func IsVar(s string) bool {
	return strings.HasPrefix(s, "$")
}

func InitBuiltinVaribles() {
	for _, item := range _builtinVars {
		VarSet(item[0], []byte(item[1]))
	}
}

func PrintGlobalVaribles() {
	_globalVarMutex.RLock()
	defer _globalVarMutex.RUnlock()
	if len(_globalVariables) > 0 {
		var data = [][]string{
			{"Var Name", "Value"},
		}
		for k, v := range _globalVariables {
			vv := fmt.Sprintf("h'%s'", Bytes2hex(v))
			data = append(data, []string{k, vv})
		}
		PrintTable(data)
	}
}
