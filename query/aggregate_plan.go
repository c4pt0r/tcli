package query

import (
	"errors"
	"fmt"
	"strings"
)

var (
	defaultAggrKey = "*"
)

type AggrPlanField struct {
	ID        int
	Name      string
	IsKey     bool
	Expr      Expression
	FuncExprs []*FunctionCallExpr
	Funcs     []AggrFunction
	Value     Column
}

type AggregatePlan struct {
	Txn           Txn
	ChildPlan     Plan
	FieldNames    []string
	FieldTypes    []Type
	Fields        []Expression
	GroupByFields []GroupByField
	AggrAll       bool
	Limit         int
	Start         int
	aggrFields    []*AggrPlanField
	aggrKeyFields []Expression
	aggrMap       map[string][]*AggrPlanField
	aggrRows      [][]*AggrPlanField
	prepared      bool
	pos           int
	skips         int
	current       int
}

func (a *AggregatePlan) listAggrFuncs(expr Expression) ([]*FunctionCallExpr, []string) {
	var (
		retExprs []*FunctionCallExpr
		retNames []string
	)

	switch e := expr.(type) {
	case *BinaryOpExpr:
		if fcexpr, names := a.listAggrFuncs(e.Left); len(fcexpr) > 0 {
			retExprs = append(retExprs, fcexpr...)
			retNames = append(retNames, names...)
		}
		if fcexpr, names := a.listAggrFuncs(e.Right); len(fcexpr) > 0 {
			retExprs = append(retExprs, fcexpr...)
			retNames = append(retNames, names...)
		}
	case *FunctionCallExpr:
		fname, err := GetFuncNameFromExpr(expr)
		if err == nil && IsAggrFunc(fname) {
			retExprs = append(retExprs, e)
			retNames = append(retNames, fname)
		}
	}
	return retExprs, retNames
}

func (a *AggregatePlan) listAggrFunctions(expr Expression) ([]*FunctionCallExpr, []AggrFunction, bool, error) {
	fcexprs, fnames := a.listAggrFuncs(expr)
	if len(fnames) == 0 {
		return nil, nil, false, nil
	}
	var functors []AggrFunction
	for i, fname := range fnames {
		functor, have := GetAggrFunctionByName(fname)
		if !have {
			return nil, nil, false, fmt.Errorf("Cannot find aggregate function: %s", fname)
		}
		if !functor.VarArgs && functor.NumArgs != len(fcexprs[i].Args) {
			return nil, nil, false, fmt.Errorf("Function %s require %d arguments but got %d", functor.Name, functor.NumArgs, len(fcexprs[i].Args))
		}
		functors = append(functors, functor.Body())
	}
	return fcexprs, functors, true, nil
}

func (a *AggregatePlan) Init() error {
	a.aggrMap = make(map[string][]*AggrPlanField)
	a.aggrRows = make([][]*AggrPlanField, 0, 10)
	a.aggrKeyFields = make([]Expression, 0, 10)
	a.aggrFields = make([]*AggrPlanField, 0, 10)
	for i, f := range a.Fields {
		var (
			aggrFuncs []AggrFunction = nil
			err       error          = nil
			name      string         = a.FieldNames[i]
			found     bool           = false
			isKey     bool           = true
			fexprs    []*FunctionCallExpr
		)
		switch e := f.(type) {
		case *FunctionCallExpr, *BinaryOpExpr:
			isKey = false
			fexprs, aggrFuncs, found, err = a.listAggrFunctions(e)
			if err != nil {
				return err
			}
			isKey = !found
		default:
			isKey = true
			a.aggrKeyFields = append(a.aggrKeyFields, f)
		}
		a.aggrFields = append(a.aggrFields, &AggrPlanField{
			ID:        i,
			Name:      name,
			IsKey:     isKey,
			Expr:      f,
			Funcs:     aggrFuncs,
			FuncExprs: fexprs,
		})
	}
	a.pos = 0
	a.skips = 0
	a.current = 0
	return a.ChildPlan.Init()
}

func (a *AggregatePlan) FieldNameList() []string {
	return a.FieldNames
}

func (a *AggregatePlan) FieldTypeList() []Type {
	return a.FieldTypes
}

func (a *AggregatePlan) String() string {
	fields := []string{}
	for _, f := range a.Fields {
		fields = append(fields, f.String())
	}
	groups := make([]string, 0, 1)
	if a.AggrAll {
		groups = append(groups, "*")
	} else {
		for _, f := range a.GroupByFields {
			groups = append(groups, f.Name)
		}
	}

	if a.Limit < 0 {
		return fmt.Sprintf("AggregatePlan{Fields = <%s>, GroupBy = <%s>}",
			strings.Join(fields, ", "),
			strings.Join(groups, ", "))
	}
	return fmt.Sprintf("AggregatePlan{Fields = <%s>, GroupBy = <%s>, Start = %d, Count = %d}",
		strings.Join(fields, ", "),
		strings.Join(groups, ", "),
		a.Start, a.Limit)
}

func (a *AggregatePlan) Explain() []string {
	ret := []string{a.String()}
	for _, plan := range a.ChildPlan.Explain() {
		ret = append(ret, plan)
	}
	return ret
}

func (a *AggregatePlan) prepare() error {
	for {
		k, v, err := a.ChildPlan.Next()
		if err != nil {
			return err
		}
		if k == nil && v == nil && err == nil {
			break
		}
		aggrKey, err := a.getAggrKey(k, v)
		if err != nil {
			return err
		}
		kvp := NewKVP(k, v)
		row, have := a.aggrMap[aggrKey]
		if !have {
			row = make([]*AggrPlanField, len(a.aggrFields))
			for i, r := range a.aggrFields {
				col := &AggrPlanField{
					ID:        r.ID,
					Name:      r.Name,
					IsKey:     r.IsKey,
					Expr:      r.Expr,
					FuncExprs: r.FuncExprs,
					Funcs:     nil,
				}
				if len(r.Funcs) > 0 {
					for _, f := range r.Funcs {
						col.Funcs = append(col.Funcs, f.Clone())
					}
				}
				if col.IsKey {
					exprResult, err := a.execExpr(kvp, col.Expr)
					if err != nil {
						return err
					}
					col.Value = exprResult
				}
				row[i] = col
			}
			a.aggrMap[aggrKey] = row
			a.aggrRows = append(a.aggrRows, row)
		}
		for _, col := range row {
			if !col.IsKey {
				fcexprs := col.FuncExprs
				if len(fcexprs) == 0 {
					return errors.New("Cannot cast expression to function call expression")
				}
				for i, fcexpr := range fcexprs {
					err = col.Funcs[i].Update(kvp, fcexpr.Args)
					if err != nil {
						return err
					}
				}
			}
		}
	}
	a.prepared = true
	return nil
}

func (a *AggregatePlan) Next() ([]Column, error) {
	if !a.prepared {
		err := a.prepare()
		if err != nil {
			return nil, err
		}
	}
	if a.Limit < 0 {
		return a.next()
	}
	for a.skips < a.Start {
		row, err := a.next()
		if err != nil {
			return nil, err
		}
		if row == nil && err == nil {
			return nil, nil
		}
		a.skips++
	}
	if a.current >= a.Limit {
		return nil, nil
	}
	row, err := a.next()
	if err != nil {
		return nil, err
	}
	if row == nil && err == nil {
		return nil, nil
	}
	a.current++
	return row, nil
}

func (a *AggregatePlan) next() ([]Column, error) {
	var err error
	if a.pos >= len(a.aggrRows) {
		return nil, nil
	}
	aggrRow := a.aggrRows[a.pos]
	a.pos++
	row := make([]Column, len(a.aggrFields))
	for i, col := range aggrRow {
		if col.IsKey {
			row[i] = col.Value
		} else {
			for i, f := range col.Funcs {
				val, err := f.Complete()
				if err != nil {
					return nil, err
				}
				col.FuncExprs[i].Result = val
			}
			row[i], err = col.Expr.Execute(NewKVP(nil, nil))
			if err != nil {
				return nil, err
			}
		}
	}
	return row, nil
}

func (a *AggregatePlan) getAggrKey(key []byte, val []byte) (string, error) {
	if a.AggrAll {
		return defaultAggrKey, nil
	}
	gkey := ""
	kvp := NewKVP(key, val)
	for _, f := range a.GroupByFields {
		eval, err := f.Expr.Execute(kvp)
		if err != nil {
			return "", err
		}
		bval, err := a.convertToBytes(eval)
		if err != nil {
			return "", err
		}
		gkey += string(bval)
	}
	return gkey, nil
}

func (a *AggregatePlan) execExpr(kvp KVPair, expr Expression) ([]byte, error) {
	result, err := expr.Execute(kvp)
	if err != nil {
		return nil, err
	}
	return a.convertToBytes(result)
}

func (a *AggregatePlan) convertToBytes(val any) ([]byte, error) {
	switch value := val.(type) {
	case bool:
		if value {
			return []byte("true"), nil
		} else {
			return []byte("false"), nil
		}
	case []byte:
		return value, nil
	case string:
		return []byte(value), nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return []byte(fmt.Sprintf("%d", value)), nil
	case float32, float64:
		return []byte(fmt.Sprintf("%f", value)), nil
	default:
		if val == nil {
			return nil, nil
		}
		return nil, errors.New("Expression result type not support")
	}
}