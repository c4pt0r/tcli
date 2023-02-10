package query

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

var (
	ErrNotEnoughOperators       = errors.New("Not enough join operators")
	ErrUnknownLeftKeyword       = errors.New("Unknown left keyword")
	ErrUnsupportCompareOperator = errors.New("Unsupport compare operator")
	ErrNoCompareExists          = errors.New("No compare expression exists")
	ErrSyntaxUnknownOperator    = errors.New("Syntax Error: unknown operator")
)

type KVPair struct {
	Key   []byte
	Value []byte
}

func NewKVP(key []byte, val []byte) KVPair {
	return KVPair{
		Key:   key,
		Value: val,
	}
}

func NewKVPStr(key string, val string) KVPair {
	return KVPair{
		Key:   []byte(key),
		Value: []byte(val),
	}
}

type FilterExec struct {
	Ast *WhereStmt
}

func (e *FilterExec) Explain() string {
	return e.Ast.Expr.String()
}

func (e *FilterExec) Filter(kvp KVPair) (bool, error) {
	ret, err := e.FilterBatch([]KVPair{kvp})
	if err != nil {
		return false, err
	}
	return ret[0], nil
}

func (e *FilterExec) FilterBatch(kvps []KVPair) ([]bool, error) {
	ret := make([]bool, len(kvps))
	for idx, kvp := range kvps {
		result, err := e.Ast.Expr.Execute(kvp)
		if err != nil {
			return nil, err
		}
		bresult, ok := result.(bool)
		if !ok {
			return nil, errors.New("Expression result is not boolean")
		}
		ret[idx] = bresult
	}
	return ret, nil
}

func (e *StringExpr) Execute(kv KVPair) (any, error) {
	return []byte(e.Data), nil
}

func (e *FieldExpr) Execute(kv KVPair) (any, error) {
	switch e.Field {
	case KeyKW:
		return kv.Key, nil
	case ValueKW:
		return kv.Value, nil
	}
	return nil, errors.New("Invalid Field")
}

func (e *BinaryOpExpr) Execute(kv KVPair) (any, error) {
	leftTp := e.Left.ReturnType()
	switch e.Op {
	case Eq:
		return e.execEqual(kv)
	case NotEq:
		return e.execNotEqual(kv)
	case PrefixMatch:
		return e.execPrefixMatch(kv)
	case RegExpMatch:
		return e.execRegexpMatch(kv)
	case And:
		return e.execAnd(kv)
	case Or:
		return e.execOr(kv)
	case Add:
		return e.execMath(kv, '+')
	case Sub:
		return e.execMath(kv, '-')
	case Mul:
		return e.execMath(kv, '*')
	case Div:
		return e.execMath(kv, '/')
	case Gt:
		switch leftTp {
		case TSTR:
			return e.execStringCompare(kv, ">")
		default:
			return e.execNumberCompare(kv, ">")
		}
	case Gte:
		switch leftTp {
		case TSTR:
			return e.execStringCompare(kv, ">=")
		default:
			return e.execNumberCompare(kv, ">=")
		}
	case Lt:
		switch leftTp {
		case TSTR:
			return e.execStringCompare(kv, "<")
		default:
			return e.execNumberCompare(kv, "<")
		}
	case Lte:
		switch leftTp {
		case TSTR:
			return e.execStringCompare(kv, "<=")
		default:
			return e.execNumberCompare(kv, "<=")
		}
	}
	return nil, errors.New("Unknown operator")
}

func (e *BinaryOpExpr) execEqual(kv KVPair) (bool, error) {
	rleft, err := e.Left.Execute(kv)
	if err != nil {
		return false, err
	}
	rright, err := e.Right.Execute(kv)
	if err != nil {
		return false, err
	}
	switch rleft.(type) {
	case string, []byte:
		left, lok := convertToByteArray(rleft)
		right, rok := convertToByteArray(rright)
		if lok && rok {
			return bytes.Equal(left, right), nil
		}
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		lint, lok := convertToInt(rleft)
		rint, rok := convertToInt(rright)
		if lok && rok {
			return lint == rint, nil
		}
	case bool:
		lbool, lok := rleft.(bool)
		rbool, rok := rright.(bool)
		if lok && rok {
			return lbool == rbool, nil
		}
	}
	return false, errors.New("Invalid = operator left type")
}

func (e *BinaryOpExpr) execNotEqual(kv KVPair) (bool, error) {
	ret, err := e.execEqual(kv)
	if err != nil {
		return false, err
	}
	return !ret, nil
}

func (e *BinaryOpExpr) execPrefixMatch(kv KVPair) (bool, error) {
	rleft, err := e.Left.Execute(kv)
	if err != nil {
		return false, err
	}
	rright, err := e.Right.Execute(kv)
	if err != nil {
		return false, err
	}
	left, lok := convertToByteArray(rleft)
	right, rok := convertToByteArray(rright)
	if !lok || !rok {
		return false, errors.New("^= left value error")
	}
	return bytes.HasPrefix(left, right), nil
}

func (e *BinaryOpExpr) execRegexpMatch(kv KVPair) (bool, error) {
	rleft, err := e.Left.Execute(kv)
	if err != nil {
		return false, err
	}
	rright, err := e.Right.Execute(kv)
	if err != nil {
		return false, err
	}
	left, lok := convertToByteArray(rleft)
	right, rok := convertToByteArray(rright)
	if !lok || !rok {
		return false, errors.New("~= left value error")
	}
	re, err := regexp.Compile(string(right))
	if err != nil {
		return false, err
	}
	return re.Match(left), nil
}

func (e *BinaryOpExpr) execAnd(kv KVPair) (bool, error) {
	rleft, err := e.Left.Execute(kv)
	if err != nil {
		return false, err
	}
	left, lok := rleft.(bool)
	if !lok {
		return false, errors.New("& left value type not bool")
	}
	if !left {
		return false, nil
	}
	rright, err := e.Right.Execute(kv)
	if err != nil {
		return false, err
	}
	right, rok := rright.(bool)
	if !rok {
		return false, errors.New("& right value type not bool")
	}
	return left && right, nil
}

func (e *BinaryOpExpr) execOr(kv KVPair) (bool, error) {
	rleft, err := e.Left.Execute(kv)
	if err != nil {
		return false, err
	}
	left, lok := rleft.(bool)
	if !lok {
		return false, errors.New("| left value type not bool")
	}
	if left {
		return true, nil
	}
	rright, err := e.Right.Execute(kv)
	if err != nil {
		return false, err
	}
	right, rok := rright.(bool)
	if !rok {
		return false, errors.New("| right value type not bool")
	}
	return left || right, nil
}

func (e *BinaryOpExpr) execMath(kv KVPair, op byte) (any, error) {
	left, err := e.Left.Execute(kv)
	if err != nil {
		return false, err
	}
	right, err := e.Right.Execute(kv)
	if err != nil {
		return false, err
	}
	return executeMathOp(left, right, op)
}

func (e *BinaryOpExpr) execNumberCompare(kv KVPair, op string) (any, error) {
	left, err := e.Left.Execute(kv)
	if err != nil {
		return false, err
	}
	right, err := e.Right.Execute(kv)
	if err != nil {
		return false, err
	}
	return execNumberCompare(left, right, op)
}

func (e *BinaryOpExpr) execStringCompare(kv KVPair, op string) (any, error) {
	left, err := e.Left.Execute(kv)
	if err != nil {
		return false, err
	}
	right, err := e.Right.Execute(kv)
	if err != nil {
		return false, err
	}
	return execStringCompare(left, right, op)
}

func (e *NotExpr) Execute(kv KVPair) (any, error) {
	rright, err := e.Right.Execute(kv)
	if err != nil {
		return nil, err
	}
	right, rok := rright.(bool)
	if !rok {
		return nil, errors.New("! right value error")
	}
	return !right, nil
}

func (e *FunctionCallExpr) Execute(kv KVPair) (any, error) {
	efname, err := e.Name.Execute(kv)
	if err != nil {
		return nil, err
	}
	fname, ok := efname.(string)
	if !ok {
		return nil, errors.New("Invalid function name")
	}
	fnameKey := strings.ToLower(fname)
	if funcObj, have := funcMap[fnameKey]; have {
		return e.executeFunc(kv, funcObj)
	}
	return nil, fmt.Errorf("Cannot find function %s", fname)
}

func (e *FunctionCallExpr) executeFunc(kv KVPair, funcObj *Function) (any, error) {
	// Check arguments
	if !funcObj.VarArgs {
		if len(e.Args) != funcObj.NumArgs {
			return nil, fmt.Errorf("Function %s require %d arguments but got %d", funcObj.Name, funcObj.NumArgs, len(e.Args))
		}
	}
	return funcObj.Body(kv, e.Args)
}

func (e *NameExpr) Execute(kv KVPair) (any, error) {
	return e.Data, nil
}

func (e *NumberExpr) Execute(kv KVPair) (any, error) {
	return e.Int, nil
}

func (e *FloatExpr) Execute(kv KVPair) (any, error) {
	return e.Float, nil
}

func (e *BoolExpr) Execute(kv KVPair) (any, error) {
	return e.Bool, nil
}
