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
	Fields     []Expression
}

func (p *ProjectionPlan) Init() error {
	return p.ChildPlan.Init()
}

func (p *ProjectionPlan) FieldNameList() []string {
	if p.AllFields {
		return []string{"Key", "Value"}
	}
	return p.FieldNames
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
	return p.processProjection(k, v)
}

func (p *ProjectionPlan) processProjection(key []byte, value []byte) ([]Column, error) {
	nFields := len(p.Fields)
	ret := make([]Column, nFields)
	kvp := NewKVP(key, value)
	for i := 0; i < nFields; i++ {
		result, err := p.Fields[i].Execute(kvp)
		if err != nil {
			return nil, err
		}
		switch value := result.(type) {
		case bool:
			if value {
				ret[i] = []byte("true")
			} else {
				ret[i] = []byte("false")
			}
		case []byte:
			ret[i] = value
		case string:
			ret[i] = []byte(value)
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			ret[i] = []byte(fmt.Sprintf("%d", value))
		case float32, float64:
			ret[i] = []byte(fmt.Sprintf("%f", value))
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
