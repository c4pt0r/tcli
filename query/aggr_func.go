package query

var (
	aggrFuncMap = map[string]*AggrFunc{
		"count": &AggrFunc{"count", 1, false, TNUMBER, newAggrCountFunc},
		"sum":   &AggrFunc{"sum", 1, false, TNUMBER, newAggrSumFunc},
	}
)

type AggrFunc struct {
	Name       string
	NumArgs    int
	VarArgs    bool
	ReturnType Type
	Body       AggrFunctor
}

type AggrFunctor func() AggrFunction

type AggrFunction interface {
	Update(kv KVPair, args []Expression) error
	Complete() (any, error)
	Clone() AggrFunction
}

func IsAggrFuncExpr(expr Expression) bool {
	fc, ok := expr.(*FunctionCallExpr)
	if !ok {
		return false
	}
	rfname, err := fc.Name.Execute(NewKVP(nil, nil))
	if err != nil {
		return false
	}
	fname, ok := rfname.(string)
	if !ok {
		return false
	}
	if _, have := aggrFuncMap[fname]; have {
		return true
	}
	return false
}

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
