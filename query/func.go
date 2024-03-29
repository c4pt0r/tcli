package query

import (
	"fmt"
	"strconv"
	"strings"
)

var (
	funcMap = map[string]*Function{
		"lower":    &Function{"lower", 1, false, TSTR, funcToLower, funcToLowerVec},
		"upper":    &Function{"upper", 1, false, TSTR, funcToUpper, funcToUpperVec},
		"int":      &Function{"int", 1, false, TNUMBER, funcToInt, funcToIntVec},
		"float":    &Function{"float", 1, false, TNUMBER, funcToFloat, funcToFloatVec},
		"str":      &Function{"str", 1, false, TSTR, funcToString, funcToStringVec},
		"is_int":   &Function{"is_int", 1, false, TBOOL, funcIsInt, funcIsIntVec},
		"is_float": &Function{"is_float", 1, false, TBOOL, funcIsFloat, funcIsFloatVec},
		"substr":   &Function{"substr", 3, false, TSTR, funcSubStr, funcSubStrVec},
		"json":     &Function{"json", 1, false, TJSON, funcJson, funcJsonVec},
		"split":    &Function{"split", 2, false, TLIST, funcSplit, funcSplitVec},
	}

	aggrFuncMap = map[string]*AggrFunc{
		"count":    &AggrFunc{"count", 1, false, TNUMBER, newAggrCountFunc},
		"sum":      &AggrFunc{"sum", 1, false, TNUMBER, newAggrSumFunc},
		"avg":      &AggrFunc{"avg", 1, false, TNUMBER, newAggrAvgFunc},
		"min":      &AggrFunc{"min", 1, false, TNUMBER, newAggrMinFunc},
		"max":      &AggrFunc{"max", 1, false, TNUMBER, newAggrMaxFunc},
		"quantile": &AggrFunc{"quantile", 2, false, TNUMBER, newAggrQuantileFunc},
	}
)

type FunctionBody func(kv KVPair, args []Expression) (any, error)
type VectorFunctionBody func(chunk []KVPair, args []Expression) ([]any, error)

type Function struct {
	Name       string
	NumArgs    int
	VarArgs    bool
	ReturnType Type
	Body       FunctionBody
	BodyVec    VectorFunctionBody
}

type AggrFunc struct {
	Name       string
	NumArgs    int
	VarArgs    bool
	ReturnType Type
	Body       AggrFunctor
}

type AggrFunctor func(args []Expression) (AggrFunction, error)

type AggrFunction interface {
	Update(kv KVPair, args []Expression) error
	Complete() (any, error)
	Clone() AggrFunction
}

func GetFuncNameFromExpr(expr Expression) (string, error) {
	fc, ok := expr.(*FunctionCallExpr)
	if !ok {
		return "", NewSyntaxError(expr.GetPos(), "Not function call expression")
	}
	rfname, err := fc.Name.Execute(NewKVP(nil, nil))
	if err != nil {
		return "", err
	}
	fname, ok := rfname.(string)
	if !ok {
		return "", NewSyntaxError(expr.GetPos(), "Invalid function name")
	}
	return strings.ToLower(fname), nil
}

func GetScalarFunction(expr Expression) (*Function, error) {
	fname, err := GetFuncNameFromExpr(expr)
	if err != nil {
		return nil, err
	}
	fobj, have := funcMap[fname]
	if !have {
		return nil, NewSyntaxError(expr.GetPos(), "Cannot find function %s", fname)
	}
	return fobj, nil
}

func GetScalarFunctionByName(name string) (*Function, bool) {
	fobj, have := funcMap[name]
	return fobj, have
}

func GetAggrFunctionByName(name string) (*AggrFunc, bool) {
	fobj, have := aggrFuncMap[name]
	return fobj, have
}

func AddScalarFunction(f *Function) {
	fname := strings.ToLower(f.Name)
	funcMap[fname] = f
}

func AddAggrFunction(f *AggrFunc) {
	fname := strings.ToLower(f.Name)
	aggrFuncMap[fname] = f
}

func IsScalarFuncExpr(expr Expression) bool {
	fname, err := GetFuncNameFromExpr(expr)
	if err != nil {
		return false
	}
	if _, have := funcMap[fname]; have {
		return true
	}
	return false
}

func IsAggrFuncExpr(expr Expression) bool {
	fname, err := GetFuncNameFromExpr(expr)
	if err != nil {
		return false
	}
	if _, have := aggrFuncMap[fname]; have {
		return true
	}
	return false
}

func IsAggrFunc(fname string) bool {
	_, have := aggrFuncMap[fname]
	return have
}

func toString(value any) string {
	switch val := value.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", val)
	case float32, float64:
		return fmt.Sprintf("%f", val)
	case bool:
		if val {
			return "true"
		}
		return "false"
	default:
		if val == nil {
			return "<nil>"
		}
		return ""
	}
}

func toInt(value any, defVal int64) int64 {
	switch val := value.(type) {
	case string:
		if ret, err := strconv.ParseInt(val, 10, 64); err == nil {
			return ret
		} else {
			if ret, err := strconv.ParseFloat(val, 64); err == nil {
				return int64(ret)
			}
			return defVal
		}
	case []byte:
		if ret, err := strconv.ParseInt(string(val), 10, 64); err == nil {
			return ret
		} else {
			if ret, err := strconv.ParseFloat(string(val), 64); err == nil {
				return int64(ret)
			}
			return defVal
		}
	case int8, int16, uint8, uint16:
		if ret, err := strconv.ParseInt(fmt.Sprintf("%d", val), 10, 64); err == nil {
			return ret
		} else {
			return defVal
		}
	case int:
		return int64(val)
	case uint:
		return int64(val)
	case int32:
		return int64(val)
	case uint32:
		return int64(val)
	case int64:
		return val
	case uint64:
		return int64(val)
	case float32:
		return int64(val)
	case float64:
		return int64(val)
	default:
		return defVal
	}
}

func toFloat(value any, defVal float64) float64 {
	switch val := value.(type) {
	case string:
		if ret, err := strconv.ParseFloat(val, 64); err == nil {
			return ret
		} else {
			return defVal
		}
	case []byte:
		if ret, err := strconv.ParseFloat(string(val), 64); err == nil {
			return ret
		} else {
			return defVal
		}
	case int8, int16, uint8, uint16:
		if ret, err := strconv.ParseFloat(fmt.Sprintf("%d", val), 64); err == nil {
			return ret
		} else {
			return defVal
		}
	case int:
		return float64(val)
	case uint:
		return float64(val)
	case int32:
		return float64(val)
	case uint32:
		return float64(val)
	case int64:
		return float64(val)
	case uint64:
		return float64(val)
	case float32:
		return float64(val)
	case float64:
		return val
	default:
		return defVal
	}
}
