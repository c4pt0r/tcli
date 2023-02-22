package query

import (
	"errors"
	"fmt"
	"strings"
)

type ProjectionPlan struct {
	Txn        Txn
	ChildPlan  Plan
	AllFields  bool
	FieldNames []string
	FieldTypes []Type
	Fields     []Expression
}

func (p *ProjectionPlan) Init() error {
	return p.ChildPlan.Init()
}

func (p *ProjectionPlan) FieldNameList() []string {
	if p.AllFields {
		return []string{"KEY", "VALUE"}
	}
	return p.FieldNames
}

func (p *ProjectionPlan) FieldTypeList() []Type {
	if p.AllFields {
		return []Type{TSTR, TSTR}
	}
	return p.FieldTypes
}

func (p *ProjectionPlan) Next() ([]Column, error) {
	k, v, err := p.ChildPlan.Next()
	if err != nil {
		return nil, err
	}
	if k == nil && v == nil && err == nil {
		return nil, nil
	}
	if p.AllFields {
		return []Column{k, v}, nil
	}
	return p.processProjection(NewKVP(k, v))
}

func (p *ProjectionPlan) Batch() ([][]Column, error) {
	kvps, err := p.ChildPlan.Batch()
	if err != nil {
		return nil, err
	}
	if len(kvps) == 0 {
		return nil, nil
	}
	ret := make([][]Column, 0, PlanBatchSize)
	if p.AllFields {
		for _, kvp := range kvps {
			ret = append(ret, []Column{kvp.Key, kvp.Value})
		}
		return ret, nil
	}
	for _, kvp := range kvps {
		row, err := p.processProjection(kvp)
		if err != nil {
			return nil, err
		}
		ret = append(ret, row)
	}
	return ret, nil
}

func (p *ProjectionPlan) processProjection(kvp KVPair) ([]Column, error) {
	nFields := len(p.Fields)
	ret := make([]Column, nFields)
	for i := 0; i < nFields; i++ {
		result, err := p.Fields[i].Execute(kvp)
		if err != nil {
			return nil, err
		}
		switch value := result.(type) {
		case bool, []byte, string,
			int, int8, int16, int32, int64,
			uint, uint8, uint16, uint32, uint64,
			float32, float64:
			ret[i] = value
		default:
			if value == nil {
				ret[i] = nil
				break
			}
			return nil, errors.New("Expression result type not support")
		}
	}
	return ret, nil
}

func (p *ProjectionPlan) String() string {
	fields := []string{}
	if p.AllFields {
		fields = append(fields, "*")
	} else {
		for _, f := range p.Fields {
			fields = append(fields, f.String())
		}
	}
	return fmt.Sprintf("ProjectionPlan{Fields = <%s>}", strings.Join(fields, ", "))
}

func (p *ProjectionPlan) Explain() []string {
	ret := []string{p.String()}
	for _, plan := range p.ChildPlan.Explain() {
		ret = append(ret, plan)
	}
	return ret
}
