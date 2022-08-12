package utils

import (
	"fmt"
	"strings"
	"sync"
)

var (
	SysVarPrintFormatKey string = "sys.printfmt"
)

var (
	_varMutex sync.RWMutex

	_globalVariables = make(map[string][]byte)
	_builtinVars     = [][]string{
		{`head`, "\x00"},
	}

	_globalSysVariables = make(map[string]string)
	_builtinSysVars     = [][]string{
		{SysVarPrintFormatKey, "table"},
	}
)

func VarGet(varname string) ([]byte, bool) {
	_varMutex.RLock()
	defer _varMutex.RUnlock()
	val, ok := _globalVariables[varname]
	return val, ok
}

func VarSet(varname string, val []byte) {
	_varMutex.Lock()
	defer _varMutex.Unlock()
	_globalVariables[varname] = append([]byte{}, val...)
}

func IsVar(s string) bool {
	return strings.HasPrefix(s, "$")
}

func InitBuiltinVaribles() {
	for _, item := range _builtinVars {
		VarSet(item[0], []byte(item[1]))
	}

	for _, item := range _builtinSysVars {
		SysVarSet(item[0], item[1])
	}
}
func PrintGlobalVaribles() {
	_varMutex.RLock()
	defer _varMutex.RUnlock()
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

func SysVarGet(varname string) (string, bool) {
	_varMutex.RLock()
	defer _varMutex.RUnlock()
	val, ok := _globalSysVariables[varname]
	return val, ok
}

func SysVarSet(varname, val string) {
	_varMutex.Lock()
	defer _varMutex.Unlock()
	_globalSysVariables[varname] = val
}

func PrintSysVaribles() {
	_varMutex.RLock()
	defer _varMutex.RUnlock()
	if len(_globalSysVariables) > 0 {
		var data = [][]string{
			{"System Varibles Name", "Value"},
		}
		for k, v := range _globalSysVariables {
			data = append(data, []string{k, string(v)})
		}
		PrintTable(data)
	}
}
