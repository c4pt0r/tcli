package utils

import (
	"strings"
	"sync"
)

var (
	_globalVariables = make(map[string][]byte)
	_globalVarMutex  sync.RWMutex

	_builtinVars = [][]string{
		[]string{`head`, `h"00"`},
		[]string{`end`, `h"fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"`},
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
		var data = [][]string{[]string{"Var Name", "Value"}}
		for k, v := range _globalVariables {
			data = append(data, []string{k, string(v)})
		}
		PrintTable(data)
	}
}
