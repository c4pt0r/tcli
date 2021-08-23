package variables

import (
	"strings"
	"sync"
)

var (
	_globalVariables = make(map[string][]byte)
	_globalVarMutex  sync.RWMutex
)

type VarExpr struct {
	VarName string
	Val     []byte
}

func Get(varname string) ([]byte, bool) {
	_globalVarMutex.RLock()
	defer _globalVarMutex.RUnlock()
	val, ok := _globalVariables[varname]
	return val, ok
}

func Set(varname string, val []byte) {
	_globalVarMutex.Lock()
	defer _globalVarMutex.Unlock()
	_globalVariables[varname] = append([]byte{}, val...)
}

func IsVar(s string) bool {
	return strings.HasPrefix(s, "$")
}
