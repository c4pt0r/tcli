package query

import "fmt"

type LimitPlan struct {
	Txn       Txn
	Start     int
	Count     int
	current   int
	skips     int
	ChildPlan Plan
}

func (p *LimitPlan) Init() error {
	p.current = 0
	p.skips = 0
	return p.ChildPlan.Init()
}

func (p *LimitPlan) Next() ([]byte, []byte, error) {
	for p.skips < p.Start {
		k, v, err := p.ChildPlan.Next()
		if err != nil {
			return nil, nil, err
		}
		if k == nil && v == nil && err == nil {
			return nil, nil, nil
		}
		p.skips++
	}
	if p.current >= p.Count {
		return nil, nil, nil
	}
	k, v, err := p.ChildPlan.Next()
	if err != nil {
		return nil, nil, err
	}
	if k == nil && v == nil && err == nil {
		return nil, nil, nil
	}

	p.current++
	return k, v, nil

}

func (p *LimitPlan) String() string {
	return fmt.Sprintf("LimitPlan{Start = %d, Count = %d}", p.Start, p.Count)
}

func (p *LimitPlan) Explain() []string {
	ret := []string{p.String()}
	for _, plan := range p.ChildPlan.Explain() {
		ret = append(ret, plan)
	}
	return ret
}

type FinalLimitPlan struct {
	Txn        Txn
	Start      int
	Count      int
	current    int
	skips      int
	ChildPlan  FinalPlan
	FieldNames []string
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
