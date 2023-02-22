package query

import (
	"errors"
	"strconv"
	"strings"
)

func funcToLowerVec(chunk []KVPair, args []Expression) ([]any, error) {
	rarg, err := args[0].ExecuteBatch(chunk)
	if err != nil {
		return nil, err
	}
	var (
		ret = make([]any, len(chunk))
	)
	for i := 0; i < len(chunk); i++ {
		arg := toString(rarg[i])
		ret[i] = strings.ToLower(arg)
	}
	return ret, nil
}

func funcToUpperVec(chunk []KVPair, args []Expression) ([]any, error) {
	rarg, err := args[0].ExecuteBatch(chunk)
	if err != nil {
		return nil, err
	}
	var (
		ret = make([]any, len(chunk))
	)
	for i := 0; i < len(chunk); i++ {
		arg := toString(rarg[i])
		ret[i] = strings.ToUpper(arg)
	}
	return ret, nil
}

func funcToIntVec(chunk []KVPair, args []Expression) ([]any, error) {
	rarg, err := args[0].ExecuteBatch(chunk)
	if err != nil {
		return nil, err
	}
	var (
		ret = make([]any, len(chunk))
	)
	for i := 0; i < len(chunk); i++ {
		ret[i] = toInt(rarg[i], 0)
	}
	return ret, nil
}

func funcToFloatVec(chunk []KVPair, args []Expression) ([]any, error) {
	rarg, err := args[0].ExecuteBatch(chunk)
	if err != nil {
		return nil, err
	}
	var (
		ret = make([]any, len(chunk))
	)
	for i := 0; i < len(chunk); i++ {
		ret[i] = toFloat(rarg[i], 0.0)
	}
	return ret, nil
}

func funcToStringVec(chunk []KVPair, args []Expression) ([]any, error) {
	rarg, err := args[0].ExecuteBatch(chunk)
	if err != nil {
		return nil, err
	}
	var (
		ret = make([]any, len(chunk))
	)
	for i := 0; i < len(chunk); i++ {
		ret[i] = toString(rarg[i])
	}
	return ret, nil
}

func funcIsIntVec(chunk []KVPair, args []Expression) ([]any, error) {
	rarg, err := args[0].ExecuteBatch(chunk)
	if err != nil {
		return nil, err
	}
	var (
		ret = make([]any, len(chunk))
	)
	for i := 0; i < len(chunk); i++ {
		ret[i] = false
		switch val := rarg[i].(type) {
		case string:
			if _, err := strconv.ParseInt(val, 10, 64); err == nil {
				ret[i] = true
			}
		case []byte:
			if _, err := strconv.ParseInt(string(val), 10, 64); err == nil {
				ret[i] = true
			}
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			ret[i] = true
		}
	}
	return ret, nil
}

func funcIsFloatVec(chunk []KVPair, args []Expression) ([]any, error) {
	rarg, err := args[0].ExecuteBatch(chunk)
	if err != nil {
		return nil, err
	}
	var (
		ret = make([]any, len(chunk))
	)
	for i := 0; i < len(chunk); i++ {
		ret[i] = false
		switch val := rarg[i].(type) {
		case string:
			if _, err := strconv.ParseFloat(val, 64); err == nil {
				ret[i] = true
			}
		case []byte:
			if _, err := strconv.ParseFloat(string(val), 64); err == nil {
				ret[i] = true
			}
		case float32, float64:
			ret[i] = true
		}
	}
	return ret, nil
}

func funcSubStrVec(chunk []KVPair, args []Expression) ([]any, error) {
	if args[1].ReturnType() != TNUMBER {
		return nil, errors.New("substr function require number type parameter for second parameter")
	}
	if args[2].ReturnType() != TNUMBER {
		return nil, errors.New("substr function require number type parameter for third parameter")
	}
	values, err := args[0].ExecuteBatch(chunk)
	if err != nil {
		return nil, err
	}
	starts, err := args[1].ExecuteBatch(chunk)
	if err != nil {
		return nil, err
	}
	lengths, err := args[2].ExecuteBatch(chunk)
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(chunk); i++ {
		val := toString(values[i])
		start := int(toInt(starts[i], 0))
		length := int(toInt(lengths[i], 0))
		vlen := len(val)
		if start > vlen-1 {
			values[i] = ""
		} else {
			length = min(length, vlen-start)
			values[i] = val[start:length]
		}
	}
	return values, nil
}
