package query

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"
)

func (e *StringExpr) ExecuteBatch(chunk []KVPair) ([]any, error) {
	ret := make([]any, len(chunk))
	for i := 0; i < len(chunk); i++ {
		ret[i] = []byte(e.Data)
	}
	return ret, nil
}

func (e *FieldExpr) ExecuteBatch(chunk []KVPair) ([]any, error) {
	if e.Field != KeyKW && e.Field != ValueKW {
		return nil, errors.New("Invalid Field")
	}
	ret := make([]any, len(chunk))
	isKey := e.Field == KeyKW
	for i := 0; i < len(chunk); i++ {
		if isKey {
			ret[i] = chunk[i].Key
		} else {
			ret[i] = chunk[i].Value
		}
	}
	return ret, nil
}

func (e *NotExpr) ExecuteBatch(chunk []KVPair) ([]any, error) {
	right, err := e.Right.ExecuteBatch(chunk)
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(chunk); i++ {
		rval, rok := right[i].(bool)
		if !rok {
			return nil, errors.New("! right value error")
		}
		right[i] = !rval
	}
	return right, nil
}

func (e *NameExpr) ExecuteBatch(chunk []KVPair) ([]any, error) {
	ret := make([]any, len(chunk))
	for i := 0; i < len(chunk); i++ {
		ret[i] = e.Data
	}
	return ret, nil
}

func (e *NumberExpr) ExecuteBatch(chunk []KVPair) ([]any, error) {
	ret := make([]any, len(chunk))
	for i := 0; i < len(chunk); i++ {
		ret[i] = e.Int
	}
	return ret, nil
}

func (e *FloatExpr) ExecuteBatch(chunk []KVPair) ([]any, error) {
	ret := make([]any, len(chunk))
	for i := 0; i < len(chunk); i++ {
		ret[i] = e.Float
	}
	return ret, nil
}

func (e *BoolExpr) ExecuteBatch(chunk []KVPair) ([]any, error) {
	ret := make([]any, len(chunk))
	for i := 0; i < len(chunk); i++ {
		ret[i] = e.Bool
	}
	return ret, nil
}

func (e *ListExpr) ExecuteBatch(chunk []KVPair) ([]any, error) {
	ret := make([]any, len(chunk))
	for i := 0; i < len(chunk); i++ {
		ret[i] = e.List
	}
	return ret, nil
}

func (e *BinaryOpExpr) ExecuteBatch(chunk []KVPair) ([]any, error) {
	leftTp := e.Left.ReturnType()
	switch e.Op {
	case Eq:
		return e.execEqualBatch(chunk, false)
	case NotEq:
		return e.execEqualBatch(chunk, true)
	case PrefixMatch:
		return e.execPrefixMatchBatch(chunk)
	case RegExpMatch:
		return e.execRegexpMatchBatch(chunk)
	case And:
		return e.execAndOrBatch(chunk, true)
	case Or:
		return e.execAndOrBatch(chunk, false)
	case Add:
		if e.Left.ReturnType() == TSTR {
			return e.execStringConcateBatch(chunk)
		}
		return e.execMathBatch(chunk, '+')
	case Sub:
		return e.execMathBatch(chunk, '-')
	case Mul:
		return e.execMathBatch(chunk, '*')
	case Div:
		return e.execMathBatch(chunk, '/')
	case Gt:
		switch leftTp {
		case TSTR:
			return e.execStringCompareBatch(chunk, ">")
		default:
			return e.execNumberCompareBatch(chunk, ">")
		}
	case Gte:
		switch leftTp {
		case TSTR:
			return e.execStringCompareBatch(chunk, ">=")
		default:
			return e.execNumberCompareBatch(chunk, ">=")
		}
	case Lt:
		switch leftTp {
		case TSTR:
			return e.execStringCompareBatch(chunk, "<")
		default:
			return e.execNumberCompareBatch(chunk, "<")
		}
	case Lte:
		switch leftTp {
		case TSTR:
			return e.execStringCompareBatch(chunk, "<=")
		default:
			return e.execNumberCompareBatch(chunk, "<=")
		}
	case In:
		switch leftTp {
		case TSTR:
			return e.execInBatch(chunk, false)
		default:
			return e.execInBatch(chunk, true)
		}
	case Between:
		switch leftTp {
		case TSTR:
			return e.execBetweenBatch(chunk, false)
		default:
			return e.execBetweenBatch(chunk, true)
		}
	}
	return nil, errors.New("Unknown operator")
}

func (e *BinaryOpExpr) execEqualBatch(chunk []KVPair, not bool) ([]any, error) {
	rleft, err := e.Left.ExecuteBatch(chunk)
	if err != nil {
		return nil, err
	}
	rright, err := e.Right.ExecuteBatch(chunk)
	if err != nil {
		return nil, err
	}
	var (
		isStr  = false
		isInt  = false
		isBool = false
	)
	if len(chunk) == 0 {
		return nil, nil
	}

	switch rleft[0].(type) {
	case string, []byte:
		isStr = true
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		isInt = true
	case bool:
		isBool = true
	default:
		return nil, errors.New("Invalid = operator left type")
	}

	for i := 0; i < len(chunk); i++ {
		if isStr {
			left, lok := convertToByteArray(rleft[i])
			right, rok := convertToByteArray(rright[i])
			if !lok || !rok {
				return nil, errors.New("Invalid = operator left type")
			}
			if not {
				rleft[i] = !bytes.Equal(left, right)
			} else {
				rleft[i] = bytes.Equal(left, right)
			}
		}
		if isInt {
			left, lok := convertToInt(rleft[i])
			right, rok := convertToInt(rright[i])
			if !lok || !rok {
				return nil, errors.New("Invalid = operator left type")
			}
			if not {
				rleft[i] = left != right
			} else {
				rleft[i] = left == right
			}
		}
		if isBool {
			left, lok := rleft[i].(bool)
			right, rok := rright[i].(bool)
			if !lok || !rok {
				return nil, errors.New("Invalid = operator left type")
			}
			if not {
				rleft[i] = left != right
			} else {
				rleft[i] = left == right
			}
		}
	}
	return rleft, nil
}

func (e *BinaryOpExpr) execPrefixMatchBatch(chunk []KVPair) ([]any, error) {
	rleft, err := e.Left.ExecuteBatch(chunk)
	if err != nil {
		return nil, err
	}
	rright, err := e.Right.ExecuteBatch(chunk)
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(chunk); i++ {
		left, lok := convertToByteArray(rleft[i])
		right, rok := convertToByteArray(rright[i])
		if !lok || !rok {
			return nil, errors.New("^= left value error")
		}
		rleft[i] = bytes.HasPrefix(left, right)
	}
	return rleft, nil
}

func (e *BinaryOpExpr) execRegexpMatchBatch(chunk []KVPair) ([]any, error) {
	rleft, err := e.Left.ExecuteBatch(chunk)
	if err != nil {
		return nil, err
	}
	rright, err := e.Right.ExecuteBatch(chunk)
	if err != nil {
		return nil, err
	}
	var (
		regexpCache = make(map[string]*regexp.Regexp)
	)
	for i := 0; i < len(chunk); i++ {
		left, lok := convertToByteArray(rleft[i])
		right, rok := convertToByteArray(rright[i])
		if !lok || !rok {
			return nil, errors.New("^= left value error")
		}
		regKey := string(right)
		reg, have := regexpCache[regKey]
		if !have {
			reg, err = regexp.Compile(regKey)
			if err != nil {
				return nil, err
			}
			regexpCache[regKey] = reg
		}
		rleft[i] = reg.Match(left)
	}
	return rleft, nil
}

func (e *BinaryOpExpr) execAndOrBatch(chunk []KVPair, and bool) ([]any, error) {
	rleft, err := e.Left.ExecuteBatch(chunk)
	if err != nil {
		return nil, err
	}
	rright, err := e.Right.ExecuteBatch(chunk)
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(chunk); i++ {
		left, lok := rleft[i].(bool)
		right, rok := rright[i].(bool)
		if !lok || !rok {
			return nil, errors.New("| left or right value type not bool")
		}
		if and {
			rleft[i] = left && right
		} else {
			rleft[i] = left || right
		}
	}
	return rleft, nil
}

func (e *BinaryOpExpr) execMathBatch(chunk []KVPair, op byte) ([]any, error) {
	rleft, err := e.Left.ExecuteBatch(chunk)
	if err != nil {
		return nil, err
	}
	rright, err := e.Right.ExecuteBatch(chunk)
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(chunk); i++ {
		val, err := executeMathOp(rleft[i], rright[i], op)
		if err != nil {
			return nil, err
		}
		rleft[i] = val
	}
	return rleft, nil
}

func (e *BinaryOpExpr) execNumberCompareBatch(chunk []KVPair, op string) ([]any, error) {
	rleft, err := e.Left.ExecuteBatch(chunk)
	if err != nil {
		return nil, err
	}
	rright, err := e.Right.ExecuteBatch(chunk)
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(chunk); i++ {
		val, err := execNumberCompare(rleft[i], rright[i], op)
		if err != nil {
			return nil, err
		}
		rleft[i] = val
	}
	return rleft, nil
}

func (e *BinaryOpExpr) execStringCompareBatch(chunk []KVPair, op string) ([]any, error) {
	rleft, err := e.Left.ExecuteBatch(chunk)
	if err != nil {
		return nil, err
	}
	rright, err := e.Right.ExecuteBatch(chunk)
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(chunk); i++ {
		val, err := execStringCompare(rleft[i], rright[i], op)
		if err != nil {
			return nil, err
		}
		rleft[i] = val
	}
	return rleft, nil
}

func (e *BinaryOpExpr) execInBatch(chunk []KVPair, number bool) ([]any, error) {
	rleft, err := e.Left.ExecuteBatch(chunk)
	if err != nil {
		return nil, err
	}
	rlist, ok := e.Right.(*ListExpr)
	if !ok {
		return nil, errors.New("operator in right expression is not list")
	}
	var (
		listValues = make([][]any, len(rlist.List))
		cmp        bool
		values     []any
		cmpRet     bool
	)

	for l, expr := range rlist.List {
		if number && expr.ReturnType() != TNUMBER {
			return nil, errors.New("operator in right expression type is not number")
		}
		if !number && expr.ReturnType() != TSTR {
			return nil, errors.New("operator in right expression type is not string")
		}
		values, err = expr.ExecuteBatch(chunk)
		if err != nil {
			return nil, err
		}
		listValues[l] = values
	}

	for i := 0; i < len(chunk); i++ {
		cmpRet = false
		for j := 0; j < len(listValues); j++ {
			lval := listValues[j][i]
			left := rleft[i]

			if number {
				cmp, err = execNumberCompare(left, lval, "=")
			} else {
				cmp, err = execStringCompare(left, lval, "=")
			}
			if err != nil {
				return nil, err
			}
			if cmp {
				cmpRet = true
				break
			}
		}
		rleft[i] = cmpRet
	}
	return rleft, nil
}

func (e *BinaryOpExpr) execBetweenBatch(chunk []KVPair, number bool) ([]any, error) {
	rleft, err := e.Left.ExecuteBatch(chunk)
	if err != nil {
		return nil, err
	}
	rlist, ok := e.Right.(*ListExpr)
	if !ok || len(rlist.List) != 2 {
		return nil, errors.New("operator in right expression is not list")
	}
	lexpr := rlist.List[0]
	uexpr := rlist.List[1]
	if !number && lexpr.ReturnType() != TSTR {
		return nil, errors.New("operator between lower boundary expression type is not string")
	}
	if !number && lexpr.ReturnType() != TSTR {
		return nil, errors.New("operator between upper boundary expression type is not string")
	}
	if number && lexpr.ReturnType() != TNUMBER {
		return nil, errors.New("operator between lower boundary expression type is not string")
	}
	if number && uexpr.ReturnType() != TNUMBER {
		return nil, errors.New("operator between upper boundary expression type is not string")
	}
	lbvals, err := lexpr.ExecuteBatch(chunk)
	if err != nil {
		return nil, err
	}
	ubvals, err := uexpr.ExecuteBatch(chunk)
	if err != nil {
		return nil, err
	}
	var (
		cmp, lcmp, ucmp bool
	)
	for i := 0; i < len(chunk); i++ {
		if number {
			cmp, err = execNumberCompare(lbvals[i], ubvals[i], "<")
		} else {
			cmp, err = execStringCompare(lbvals[i], ubvals[i], "<")
		}
		if err != nil {
			return nil, err
		}
		if !cmp {
			return nil, errors.New("operator between lower boundary is greater than upper boundary.")
		}
		if number {
			lcmp, err = execNumberCompare(lbvals[i], rleft[i], "<=")
		} else {
			lcmp, err = execStringCompare(lbvals[i], rleft[i], "<=")
		}
		if err != nil {
			return nil, err
		}
		// left < lower, next
		if !lcmp {
			rleft[i] = false
			continue
		}
		if number {
			ucmp, err = execNumberCompare(rleft[i], ubvals[i], "<=")
		} else {
			ucmp, err = execStringCompare(rleft[i], ubvals[i], "<=")
		}
		if err != nil {
			return nil, err
		}
		// left < upper
		rleft[i] = ucmp
	}
	return rleft, nil
}

func (e *BinaryOpExpr) execStringConcateBatch(chunk []KVPair) ([]any, error) {
	left, err := e.Left.ExecuteBatch(chunk)
	if err != nil {
		return nil, err
	}
	right, err := e.Right.ExecuteBatch(chunk)
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(chunk); i++ {
		lval, lok := convertToByteArray(left[i])
		rval, rok := convertToByteArray(right[i])
		if !lok || !rok {
			return nil, errors.New("+ operator left or right expression not string")
		}
		cval := make([]byte, 0, len(lval)+len(rval))
		cval = append(cval, lval...)
		cval = append(cval, rval...)
		left[i] = cval
	}
	return left, nil
}

func (e *FunctionCallExpr) ExecuteBatch(chunk []KVPair) ([]any, error) {
	var (
		ret = make([]any, len(chunk))
	)
	if e.Result != nil {
		for i := 0; i < len(chunk); i++ {
			ret[i] = e.Result
		}
		return ret, nil
	}

	funcObj, err := GetScalarFunction(e)
	if err != nil {
		return nil, err
	}
	if !funcObj.VarArgs && len(e.Args) != funcObj.NumArgs {
		return nil, fmt.Errorf("Function %s require %d arguments but got %d", funcObj.Name, funcObj.NumArgs, len(e.Args))
	}
	return e.executeFuncBatch(funcObj, chunk)
}

func (e *FunctionCallExpr) executeFuncBatch(funcObj *Function, chunk []KVPair) ([]any, error) {
	if funcObj.BodyVec != nil {
		return funcObj.BodyVec(chunk, e.Args)
	}

	var (
		ret = make([]any, len(chunk))
		err error
	)
	for i := 0; i < len(chunk); i++ {
		ret[i], err = funcObj.Body(chunk[i], e.Args)
		if err != nil {
			return nil, err
		}
	}
	return ret, nil
}
