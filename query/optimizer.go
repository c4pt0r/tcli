package query

import (
	"errors"
	"fmt"
)

type Optimizer struct {
	Query  string
	stmt   *SelectStmt
	filter *FilterExec
}

func NewOptimizer(query string) *Optimizer {
	return &Optimizer{
		Query: query,
	}
}

func (o *Optimizer) init() error {
	p := NewParser(o.Query)
	stmt, err := p.Parse()
	if err != nil {
		return err
	}
	o.stmt = stmt
	o.optimizeExpressions()
	o.filter = &FilterExec{
		Ast: stmt.Where,
	}
	return nil
}

func (o *Optimizer) optimizeExpressions() {
	eo := ExpressionOptimizer{
		Root: o.stmt.Where.Expr,
	}
	o.stmt.Where.Expr = eo.Optimize()
	for i, field := range o.stmt.Fields {
		fmt.Println("Before opt", field)
		eo.Root = field
		o.stmt.Fields[i] = eo.Optimize()
		fmt.Println("After opt", o.stmt.Fields[i])
	}
}

func (o *Optimizer) findAggrFunc(expr Expression) bool {
	switch e := expr.(type) {
	case *BinaryOpExpr:
		if o.findAggrFunc(e.Left) {
			return true
		}
		if o.findAggrFunc(e.Right) {
			return true
		}
	case *FunctionCallExpr:
		return IsAggrFuncExpr(expr)
	}
	return false
}

func (o *Optimizer) buildFinalPlan(t Txn, fp Plan) (FinalPlan, error) {
	hasAggr := false
	aggrFields := 0
	aggrAll := true
	for _, field := range o.stmt.Fields {
		if o.findAggrFunc(field) {
			hasAggr = true
			aggrFields++
		}
	}
	var ffp FinalPlan
	if !hasAggr {
		ffp = &ProjectionPlan{
			Txn:        t,
			ChildPlan:  fp,
			AllFields:  o.stmt.AllFields,
			FieldNames: o.stmt.FieldNames,
			FieldTypes: o.stmt.FieldTypes,
			Fields:     o.stmt.Fields,
		}

		// Build order
		if o.stmt.Order != nil {
			ffp = o.buildFinalOrderPlan(t, ffp, false)
		}

		// Build limit
		if o.stmt.Limit != nil {
			ffp = o.buildFinalLimitPlan(t, ffp)
		}

		return ffp, nil
	}

	// Update limit
	limit := -1
	start := 0
	doNotBuildLimit := false
	// no order by only has limit
	if o.stmt.Limit != nil && o.stmt.Order == nil {
		doNotBuildLimit = true
		start = o.stmt.Limit.Start
		limit = o.stmt.Limit.Count
	}
	var groupByFields []GroupByField = nil
	if o.stmt.GroupBy != nil {
		groupByFields = o.stmt.GroupBy.Fields
		aggrAll = false
	} else {
		aggrAll = true
	}

	if aggrFields+len(groupByFields) < len(o.stmt.Fields) {
		return nil, errors.New("Syntax error: Missing aggregate fields in group by")
	}

	ffp = &AggregatePlan{
		Txn:           t,
		ChildPlan:     fp,
		AggrAll:       aggrAll,
		FieldNames:    o.stmt.FieldNames,
		FieldTypes:    o.stmt.FieldTypes,
		Fields:        o.stmt.Fields,
		GroupByFields: groupByFields,
		Limit:         limit,
		Start:         start,
	}

	if o.stmt.Order != nil {
		ffp = o.buildFinalOrderPlan(t, ffp, true)
	}

	if o.stmt.Limit != nil && !doNotBuildLimit {
		ffp = o.buildFinalLimitPlan(t, ffp)
	}
	return ffp, nil
}

func (o *Optimizer) BuildPlan(t Txn) (FinalPlan, error) {
	err := o.init()
	if err != nil {
		return nil, err
	}

	// Build Scan
	fp := o.buildScanPlan(t)

	// Just build an empty result plan so we can
	// ignore order and limit plan just return
	// the projection plan with empty result plan
	if _, ok := fp.(*EmptyResultPlan); ok {
		ret, err := o.buildFinalPlan(t, fp)
		if err != nil {
			return nil, err
		}
		err = ret.Init()
		if err != nil {
			return nil, err
		}
		return ret, nil
	}

	ret, err := o.buildFinalPlan(t, fp)
	if err != nil {
		return nil, err
	}
	err = ret.Init()
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (o *Optimizer) buildFinalLimitPlan(t Txn, ffp FinalPlan) FinalPlan {
	return &FinalLimitPlan{
		Txn:        t,
		Start:      o.stmt.Limit.Start,
		Count:      o.stmt.Limit.Count,
		FieldNames: ffp.FieldNameList(),
		FieldTypes: ffp.FieldTypeList(),
		ChildPlan:  ffp,
	}
}

func (o *Optimizer) buildFinalOrderPlan(t Txn, ffp FinalPlan, hasAggr bool) FinalPlan {
	if !hasAggr && len(o.stmt.Order.Orders) == 1 {
		order := o.stmt.Order.Orders[0]
		switch expr := order.Field.(type) {
		case *FieldExpr:
			// If order by key asc just ignore it
			if expr.Field == KeyKW && order.Order == ASC {
				return ffp
			}
		}
	}
	return &FinalOrderPlan{
		Txn:        t,
		Orders:     o.stmt.Order.Orders,
		FieldNames: ffp.FieldNameList(),
		FieldTypes: ffp.FieldTypeList(),
		ChildPlan:  ffp,
	}
}

func (o *Optimizer) buildScanPlan(t Txn) Plan {
	fopt := NewFilterOptimizer(o.filter.Ast, t, o.filter)
	return fopt.Optimize()
}
