package query

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
	o.filter = &FilterExec{
		Ast: stmt.Where,
	}
	return nil
}

func (o *Optimizer) BuildPlan(t *Txn) (*ProjectionPlan, error) {
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
		if err = fp.Init(); err != nil {
			return nil, err
		}
		return &ProjectionPlan{
			Txn:       t,
			ChildPlan: fp,
			AllFields: o.stmt.AllFields,
			Fields:    o.stmt.Fields,
		}, nil
	}

	// Build order
	if o.stmt.Order != nil {
		fp = o.buildOrderPlan(t, fp)
	}

	// Build limit
	if o.stmt.Limit != nil {
		fp = o.buildLimitPlan(t, fp)
	}

	if err = fp.Init(); err != nil {
		return nil, err
	}

	return &ProjectionPlan{
		Txn:       t,
		ChildPlan: fp,
		AllFields: o.stmt.AllFields,
		Fields:    o.stmt.Fields,
	}, nil
}

func (o *Optimizer) buildLimitPlan(t *Txn, fp Plan) Plan {
	return &LimitPlan{
		Txn:       t,
		Start:     o.stmt.Limit.Start,
		Count:     o.stmt.Limit.Count,
		ChildPlan: fp,
	}
}

func (o *Optimizer) buildOrderPlan(t *Txn, fp Plan) Plan {
	if len(o.stmt.Order.Orders) == 1 {
		order := o.stmt.Order.Orders[0]
		switch expr := order.Field.(type) {
		case *FieldExpr:
			// If order by key asc just ignore it
			if expr.Field == KeyKW && order.Order == ASC {
				return fp
			}
		}
	}
	return &OrderPlan{
		Txn:       t,
		Orders:    o.stmt.Order.Orders,
		ChildPlan: fp,
	}
}

func (o *Optimizer) buildScanPlan(t *Txn) Plan {
	fopt := NewFilterOptimizer(o.filter.Ast, t, o.filter)
	return fopt.Optimize()
}
