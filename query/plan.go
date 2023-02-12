package query

type FinalPlan interface {
	String() string
	Explain() []string
	Init() error
	Next() ([]Column, error)
	FieldNameList() []string
}

type Plan interface {
	String() string
	Explain() []string
	Init() error
	Next() (key []byte, value []byte, err error)
}

var (
	_ Plan = (*FullScanPlan)(nil)
	_ Plan = (*EmptyResultPlan)(nil)
	_ Plan = (*RangeScanPlan)(nil)
	_ Plan = (*PrefixScanPlan)(nil)
	_ Plan = (*MultiGetPlan)(nil)
	_ Plan = (*LimitPlan)(nil)
	_ Plan = (*OrderPlan)(nil)

	_ FinalPlan = (*ProjectionPlan)(nil)
	_ FinalPlan = (*AggregatePlan)(nil)
	_ FinalPlan = (*FinalOrderPlan)(nil)
	_ FinalPlan = (*FinalLimitPlan)(nil)
)

type Column []byte

type EmptyResultPlan struct {
	Txn Txn
}

func NewEmptyResultPlan(t Txn, f *FilterExec) Plan {
	return &EmptyResultPlan{
		Txn: t,
	}
}

func (p *EmptyResultPlan) Init() error {
	return nil
}

func (p *EmptyResultPlan) Next() ([]byte, []byte, error) {
	return nil, nil, nil
}

func (p *EmptyResultPlan) String() string {
	return "EmptyResultPlan"
}

func (p *EmptyResultPlan) Explain() []string {
	return []string{p.String()}
}
