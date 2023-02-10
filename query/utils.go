package query

import (
	"bytes"
	"errors"
)

func BuildExecutor(query string) (*SelectStmt, *FilterExec, error) {
	p := NewParser(query)
	expr, err := p.Parse()
	if err != nil {
		return nil, nil, err
	}
	return expr, &FilterExec{
		Ast: expr.Where,
	}, nil
}

func convertToByteArray(value any) ([]byte, bool) {
	switch ret := value.(type) {
	case []byte:
		return ret, true
	case string:
		return []byte(ret), true
	default:
		return nil, false
	}
}

func convertToInt(value any) (int64, bool) {
	switch ret := value.(type) {
	case int:
		return int64(ret), true
	case int8:
		return int64(ret), true
	case int16:
		return int64(ret), true
	case int32:
		return int64(ret), true
	case int64:
		return ret, true
	case uint:
		return int64(ret), true
	case uint8:
		return int64(ret), true
	case uint16:
		return int64(ret), true
	case uint32:
		return int64(ret), true
	case uint64:
		return int64(ret), true
	default:
		return 0, false
	}
}

func convertToFloat(value any) (float64, bool) {
	switch ret := value.(type) {
	case float32:
		return float64(ret), true
	case float64:
		return ret, true
	default:
		return 0, false
	}
}

func executeMathOp(left any, right any, op byte) (any, error) {
	lint, liok := convertToInt(left)
	rint, riok := convertToInt(right)
	if liok && riok {
		switch op {
		case '+':
			return lint + rint, nil
		case '-':
			return lint - rint, nil
		case '*':
			return lint * rint, nil
		case '/':
			return lint / rint, nil
		default:
			return 0.0, errors.New("Unknown operator")
		}
	}
	// Float
	lfloat, lfok := convertToFloat(left)
	rfloat, rfok := convertToFloat(right)
	if lfok && rfok {
		switch op {
		case '+':
			return lfloat + rfloat, nil
		case '-':
			return lfloat - rfloat, nil
		case '*':
			return lfloat * rfloat, nil
		case '/':
			return lfloat / rfloat, nil
		default:
			return 0.0, errors.New("Unknown operator")
		}
	}

	var (
		lfval float64
		rfval float64
	)
	if liok && rfok {
		lfval = float64(lint)
		rfval = rfloat
	} else if lfok && riok {
		lfval = lfloat
		rfval = float64(rint)
	} else {
		return 0.0, errors.New("Invalid operator left or right parameter type")
	}
	switch op {
	case '+':
		return lfval + rfval, nil
	case '-':
		return lfval - rfval, nil
	case '*':
		return lfval * rfval, nil
	case '/':
		return lfval / rfval, nil
	default:
		return 0.0, errors.New("Unknown operator")
	}
}

func execNumberCompare(left any, right any, op string) (bool, error) {
	lint, liok := convertToInt(left)
	rint, riok := convertToInt(right)
	if liok && riok {
		switch op {
		case ">":
			return lint > rint, nil
		case ">=":
			return lint >= rint, nil
		case "<":
			return lint < rint, nil
		case "<=":
			return lint <= rint, nil
		}
	}

	lfloat, lfok := convertToFloat(left)
	rfloat, rfok := convertToFloat(right)
	if liok && rfok {
		rfloat = float64(rint)
	} else if lfok && riok {
		lfloat = float64(lint)
	} else {
		return false, errors.New("Invalid operator left or right parameter type")
	}
	switch op {
	case ">":
		return lfloat > rfloat, nil
	case ">=":
		return lfloat >= rfloat, nil
	case "<":
		return lfloat < rfloat, nil
	case "<=":
		return lfloat <= rfloat, nil
	}
	return false, errors.New("Unknown operator")
}

func execStringCompare(left any, right any, op string) (bool, error) {
	lstr, lsok := convertToByteArray(left)
	rstr, rsok := convertToByteArray(right)
	if lsok && rsok {
		cmpret := bytes.Compare(lstr, rstr)
		switch op {
		case ">":
			return cmpret > 0, nil
		case ">=":
			return cmpret >= 0, nil
		case "<":
			return cmpret < 0, nil
		case "<=":
			return cmpret <= 0, nil
		default:
			return false, errors.New("Unknown operator")
		}
	}

	return false, errors.New("Invalid operator left or right parameter type")
}
