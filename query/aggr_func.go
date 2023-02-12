package query

import (
	"fmt"
	"strconv"
)

var (
	_ AggrFunction = (*aggrCountFunc)(nil)
	_ AggrFunction = (*aggrSumFunc)(nil)
)

// Aggr Count
type aggrCountFunc struct {
	counter int64
}

func newAggrCountFunc() AggrFunction {
	return &aggrCountFunc{counter: 0}
}

func (f *aggrCountFunc) Update(kv KVPair, args []Expression) error {
	f.counter++
	return nil
}

func (f *aggrCountFunc) Complete() (any, error) {
	return f.counter, nil
}

func (f *aggrCountFunc) Clone() AggrFunction {
	return &aggrCountFunc{
		counter: 0,
	}
}

// Aggr Sum
type aggrSumFunc struct {
	isum    int64
	fsum    float64
	isFloat bool
}

func newAggrSumFunc() AggrFunction {
	return &aggrSumFunc{
		isum:    0,
		fsum:    0.0,
		isFloat: false,
	}
}

func convertToNumber(value any) (int64, float64, bool) {
	switch val := value.(type) {
	case string:
		ival, err := strconv.ParseInt(val, 10, 64)
		if err == nil {
			return ival, float64(ival), false
		}
		fval, err := strconv.ParseFloat(val, 64)
		if err == nil {
			return int64(fval), fval, true
		}
	case []byte:
		ival, err := strconv.ParseInt(string(val), 10, 64)
		if err == nil {
			return ival, float64(ival), false
		}
		fval, err := strconv.ParseFloat(string(val), 64)
		if err == nil {
			return int64(fval), fval, true
		}
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		ival, err := strconv.ParseInt(fmt.Sprintf("%d", val), 10, 64)
		if err == nil {
			return ival, float64(ival), false
		}
	case float32:
		return int64(val), float64(val), true
	case float64:
		return int64(val), val, true
	case bool:
		if val {
			return 1, 1.0, false
		}
	}
	return 0, 0.0, false
}

func (f *aggrSumFunc) Update(kv KVPair, args []Expression) error {
	rarg, err := args[0].Execute(kv)
	if err != nil {
		return err
	}
	ival, fval, isFloat := convertToNumber(rarg)
	f.isum += ival
	f.fsum += fval
	if !f.isFloat && isFloat {
		f.isFloat = true
	}
	return nil
}

func (f *aggrSumFunc) Complete() (any, error) {
	if f.isFloat {
		return f.fsum, nil
	}
	return f.isum, nil
}

func (f *aggrSumFunc) Clone() AggrFunction {
	return &aggrSumFunc{
		isum:    0,
		fsum:    0.0,
		isFloat: false,
	}
}

// Aggr Avg
type aggrAvgFunc struct {
	isum    int64
	fsum    float64
	count   int64
	isFloat bool
}

func newAggrAvgFunc() AggrFunction {
	return &aggrAvgFunc{
		isum:    0,
		fsum:    0.0,
		count:   0,
		isFloat: false,
	}
}

func (f *aggrAvgFunc) Update(kv KVPair, args []Expression) error {
	rarg, err := args[0].Execute(kv)
	if err != nil {
		return err
	}
	ival, fval, isFloat := convertToNumber(rarg)
	f.isum += ival
	f.fsum += fval
	if !f.isFloat && isFloat {
		f.isFloat = true
	}
	f.count++
	return nil
}

func (f *aggrAvgFunc) Complete() (any, error) {
	if f.isFloat {
		return f.fsum / float64(f.count), nil
	}
	return float64(f.isum) / float64(f.count), nil
}

func (f *aggrAvgFunc) Clone() AggrFunction {
	return &aggrAvgFunc{
		isum:    0,
		fsum:    0.0,
		count:   0,
		isFloat: false,
	}
}
