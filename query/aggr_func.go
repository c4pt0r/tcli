package query

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
	sum int64
}

func newAggrSumFunc() AggrFunction {
	return &aggrSumFunc{
		sum: 0,
	}
}

func (f *aggrSumFunc) Update(kv KVPair, args []Expression) error {
	rarg, err := args[0].Execute(kv)
	if err != nil {
		return err
	}
	arg := toInt(rarg, 0)
	f.sum += arg
	return nil
}

func (f *aggrSumFunc) Complete() (any, error) {
	return f.sum, nil
}

func (f *aggrSumFunc) Clone() AggrFunction {
	return &aggrSumFunc{
		sum: 0,
	}
}

// Aggr Avg
type aggrAvgFunc struct {
	sum   int64
	count int64
}

func newAggrAvgFunc() AggrFunction {
	return &aggrAvgFunc{
		sum:   0,
		count: 0,
	}
}

func (f *aggrAvgFunc) Update(kv KVPair, args []Expression) error {
	rarg, err := args[0].Execute(kv)
	if err != nil {
		return err
	}
	arg := toInt(rarg, 0)
	f.sum += arg
	f.count++
	return nil
}

func (f *aggrAvgFunc) Complete() (any, error) {
	return float64(f.sum) / float64(f.count), nil
}

func (f *aggrAvgFunc) Clone() AggrFunction {
	return &aggrAvgFunc{
		sum:   0,
		count: 0,
	}
}
