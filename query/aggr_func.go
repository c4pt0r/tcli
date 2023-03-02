package query

import (
	"fmt"
	"strconv"
)

var (
	_ AggrFunction = (*aggrCountFunc)(nil)
	_ AggrFunction = (*aggrSumFunc)(nil)
	_ AggrFunction = (*aggrAvgFunc)(nil)
	_ AggrFunction = (*aggrMinFunc)(nil)
	_ AggrFunction = (*aggrMaxFunc)(nil)
)

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
	return newAggrCountFunc()
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
	return newAggrSumFunc()
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
	return newAggrAvgFunc()
}

// Aggr Min
type aggrMinFunc struct {
	imin    int64
	fmin    float64
	isFloat bool
	first   bool
}

func newAggrMinFunc() AggrFunction {
	return &aggrMinFunc{
		imin:    0,
		fmin:    0.0,
		isFloat: false,
		first:   false,
	}
}

func (f *aggrMinFunc) Update(kv KVPair, args []Expression) error {
	rarg, err := args[0].Execute(kv)
	if err != nil {
		return err
	}
	ival, fval, isFloat := convertToNumber(rarg)
	if !f.first {
		f.first = true
		f.imin = ival
		f.fmin = fval
		f.isFloat = isFloat
		return nil
	}
	if f.isFloat {
		if f.fmin > fval {
			f.imin = ival
			f.fmin = fval
			f.isFloat = isFloat
		}
	} else {
		if f.imin > ival {
			f.imin = ival
			f.fmin = fval
			f.isFloat = isFloat
		}
	}
	return nil
}

func (f *aggrMinFunc) Complete() (any, error) {
	if f.isFloat {
		return f.fmin, nil
	}
	return f.imin, nil
}

func (f *aggrMinFunc) Clone() AggrFunction {
	return newAggrMinFunc()
}

// Aggr Max
type aggrMaxFunc struct {
	imax    int64
	fmax    float64
	isFloat bool
	first   bool
}

func newAggrMaxFunc() AggrFunction {
	return &aggrMaxFunc{
		imax:    0,
		fmax:    0.0,
		isFloat: false,
		first:   false,
	}
}

func (f *aggrMaxFunc) Update(kv KVPair, args []Expression) error {
	rarg, err := args[0].Execute(kv)
	if err != nil {
		return err
	}
	ival, fval, isFloat := convertToNumber(rarg)
	if !f.first {
		f.first = true
		f.imax = ival
		f.fmax = fval
		f.isFloat = isFloat
		return nil
	}
	if f.isFloat {
		if f.fmax < fval {
			f.imax = ival
			f.fmax = fval
			f.isFloat = isFloat
		}
	} else {
		if f.imax < ival {
			f.imax = ival
			f.fmax = fval
			f.isFloat = isFloat
		}
	}
	return nil
}

func (f *aggrMaxFunc) Complete() (any, error) {
	if f.isFloat {
		return f.fmax, nil
	}
	return f.imax, nil
}

func (f *aggrMaxFunc) Clone() AggrFunction {
	return newAggrMaxFunc()
}
