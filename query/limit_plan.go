package query

import "fmt"

type FinalLimitPlan struct {
	Txn        Txn
	Start      int
	Count      int
	current    int
	skips      int
	ChildPlan  FinalPlan
	FieldNames []string
	FieldTypes []Type
}

func (p *FinalLimitPlan) Init() error {
	p.current = 0
	p.skips = 0
	return p.ChildPlan.Init()
}

func (p *FinalLimitPlan) Next() ([]Column, error) {
	for p.skips < p.Start {
		cols, err := p.ChildPlan.Next()
		if err != nil {
			return nil, err
		}
		if cols == nil && err == nil {
			return nil, nil
		}
		p.skips++
	}
	if p.current >= p.Count {
		return nil, nil
	}
	cols, err := p.ChildPlan.Next()
	if err != nil {
		return nil, err
	}
	if cols == nil && err == nil {
		return nil, nil
	}

	p.current++
	return cols, nil

}

func (p *FinalLimitPlan) String() string {
	return fmt.Sprintf("LimitPlan{Start = %d, Count = %d}", p.Start, p.Count)
}

func (p *FinalLimitPlan) Explain() []string {
	ret := []string{p.String()}
	for _, plan := range p.ChildPlan.Explain() {
		ret = append(ret, plan)
	}
	return ret
}

func (p *FinalLimitPlan) FieldNameList() []string {
	return p.FieldNames
}

func (p *FinalLimitPlan) FieldTypeList() []Type {
	return p.FieldTypes
}
