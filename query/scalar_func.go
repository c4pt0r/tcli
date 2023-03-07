package query

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

func funcToLower(kv KVPair, args []Expression) (any, error) {
	rarg, err := args[0].Execute(kv)
	if err != nil {
		return nil, err
	}
	arg := toString(rarg)
	return strings.ToLower(arg), nil
}

func funcToUpper(kv KVPair, args []Expression) (any, error) {
	rarg, err := args[0].Execute(kv)
	if err != nil {
		return nil, err
	}
	arg := toString(rarg)
	return strings.ToUpper(arg), nil
}

func funcToInt(kv KVPair, args []Expression) (any, error) {
	rarg, err := args[0].Execute(kv)
	if err != nil {
		return nil, err
	}
	ret := toInt(rarg, 0)
	return ret, nil
}

func funcToFloat(kv KVPair, args []Expression) (any, error) {
	rarg, err := args[0].Execute(kv)
	if err != nil {
		return nil, err
	}
	ret := toFloat(rarg, 0.0)
	return ret, nil
}

func funcToString(kv KVPair, args []Expression) (any, error) {
	rarg, err := args[0].Execute(kv)
	if err != nil {
		return nil, err
	}
	ret := toString(rarg)
	return ret, nil
}

func funcIsInt(kv KVPair, args []Expression) (any, error) {
	rarg, err := args[0].Execute(kv)
	if err != nil {
		return nil, err
	}
	switch val := rarg.(type) {
	case string:
		if _, err := strconv.ParseInt(val, 10, 64); err == nil {
			return true, nil
		}
	case []byte:
		if _, err := strconv.ParseInt(string(val), 10, 64); err == nil {
			return true, nil
		}
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return true, nil
	}
	return false, nil
}

func funcIsFloat(kv KVPair, args []Expression) (any, error) {
	rarg, err := args[0].Execute(kv)
	if err != nil {
		return nil, err
	}
	switch val := rarg.(type) {
	case string:
		if _, err := strconv.ParseFloat(val, 64); err == nil {
			return true, nil
		}
	case []byte:
		if _, err := strconv.ParseFloat(string(val), 64); err == nil {
			return true, nil
		}
	case float32, float64:
		return true, nil
	}
	return false, nil
}

func funcSubStr(kv KVPair, args []Expression) (any, error) {
	rarg, err := args[0].Execute(kv)
	if err != nil {
		return nil, err
	}
	val := toString(rarg)
	if args[1].ReturnType() != TNUMBER {
		return nil, errors.New("substr function require number type parameter for second parameter")
	}
	if args[2].ReturnType() != TNUMBER {
		return nil, errors.New("substr function require number type parameter for third parameter")
	}
	rarg, err = args[1].Execute(kv)
	if err != nil {
		return nil, err
	}
	start := int(toInt(rarg, 0))
	rarg, err = args[2].Execute(kv)
	if err != nil {
		return nil, err
	}
	length := int(toInt(rarg, 0))
	vlen := len(val)
	if start > vlen-1 {
		return "", nil
	}
	length = min(length, vlen-start)
	return val[start:length], nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type JSON map[string]any

func funcJson(kv KVPair, args []Expression) (any, error) {
	rarg, err := args[0].Execute(kv)
	if err != nil {
		return nil, err
	}
	jsonData, ok := convertToByteArray(rarg)
	if !ok {
		return nil, fmt.Errorf("Cannot convert to byte array")
	}
	ret := make(JSON)
	json.Unmarshal(jsonData, &ret)
	return ret, nil
}
