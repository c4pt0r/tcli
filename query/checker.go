package query

func (e *BinaryOpExpr) Check() error {
	if err := e.Left.Check(); err != nil {
		return err
	}
	if err := e.Right.Check(); err != nil {
		return err
	}
	switch e.Op {
	case And, Or:
		return e.checkWithAndOr()
	case Not:
		return NewSyntaxError(e.GetPos(), "Invalid operator !")
	case Add, Sub, Mul, Div:
		return e.checkWithMath()
	case In:
		return e.checkWithIn()
	case Between:
		return e.checkWithBetween()
	default:
		return e.checkWithCompares()
	}
}

func (e *BinaryOpExpr) checkWithAndOr() error {
	op := OperatorToString[e.Op]
	switch exp := e.Left.(type) {
	case *BinaryOpExpr, *FunctionCallExpr, *NotExpr:
		if e.Left.ReturnType() != TBOOL {
			return NewSyntaxError(e.Left.GetPos(), "%s operator has wrong type of left expression %s", op, exp)
		}
	default:
		return NewSyntaxError(e.Left.GetPos(), "%s operator with invalid left expression %s", op, exp)
	}

	switch exp := e.Right.(type) {
	case *BinaryOpExpr, *FunctionCallExpr, *NotExpr:
		if exp.ReturnType() != TBOOL {
			return NewSyntaxError(e.Right.GetPos(), "%s operator has wrong type of right expression %s", op, exp)
		}
	default:
		return NewSyntaxError(e.Right.GetPos(), "%s operator with invalid right expression %s", op, exp)
	}
	return nil
}

func (e *BinaryOpExpr) checkWithMath() error {
	op := OperatorToString[e.Op]
	lstring := false
	rstring := false
	switch exp := e.Left.(type) {
	case *BinaryOpExpr, *FunctionCallExpr, *NumberExpr, *FloatExpr:
		if e.Left.ReturnType() != TNUMBER {
			if e.Left.ReturnType() == TSTR {
				lstring = true
			} else {
				return NewSyntaxError(e.Left.GetPos(), "%s operator has wrong type of left expression %s", op, exp)
			}
		}
	case *StringExpr, *FieldExpr, *FieldAccessExpr:
		lstring = true
	default:
		return NewSyntaxError(e.Left.GetPos(), "%s operator with invalid left expression %s", op, exp)
	}

	switch exp := e.Right.(type) {
	case *BinaryOpExpr, *FunctionCallExpr, *NumberExpr, *FloatExpr:
		if e.Right.ReturnType() != TNUMBER {
			if e.Right.ReturnType() == TSTR {
				rstring = true
			} else {
				return NewSyntaxError(e.Right.GetPos(), "%s operator has wrong type of right expression %s", op, exp)
			}
		}
	case *StringExpr, *FieldExpr, *FieldAccessExpr:
		rstring = true
	default:
		return NewSyntaxError(e.Right.GetPos(), "%s operator with invalid right expression %s", op, exp)
	}

	if op == "+" && lstring && rstring {
	} else {
		if lstring {
			return NewSyntaxError(e.Left.GetPos(), "%s operator with invalid left expression %s", op, e.Left)
		}
		if rstring {
			return NewSyntaxError(e.Right.GetPos(), "%s operator with invalid right expression %s", op, e.Left)
		}
	}
	if op == "/" {
		switch rval := e.Right.(type) {
		case *NumberExpr:
			if rval.Int == 0 {
				return NewSyntaxError(e.Right.GetPos(), "/ operator divide by zero")
			}
		case *FloatExpr:
			if rval.Float == 0.0 {
				return NewSyntaxError(e.Right.GetPos(), "/ operator divide by zero")
			}
		}
	}
	return nil
}

func (e *BinaryOpExpr) checkWithCompares() error {
	var (
		numKeyFieldExpr   = 0
		numValueFieldExpr = 0
		numCallExpr       = 0
	)
	op := OperatorToString[e.Op]

	switch exp := e.Left.(type) {
	case *FieldExpr:
		switch exp.Field {
		case KeyKW:
			numKeyFieldExpr++
		case ValueKW:
			numValueFieldExpr++
		}
	case *FunctionCallExpr:
		numCallExpr++
	case *StringExpr, *BoolExpr, *NumberExpr, *FloatExpr, *BinaryOpExpr, *FieldAccessExpr:
	default:
		return NewSyntaxError(e.Left.GetPos(), "%s operator with invalid left expression", op)
	}

	switch exp := e.Right.(type) {
	case *FieldExpr:
		switch exp.Field {
		case KeyKW:
			numKeyFieldExpr++
		case ValueKW:
			numValueFieldExpr++
		}
	case *FunctionCallExpr:
		numCallExpr++
	case *StringExpr, *BoolExpr, *NumberExpr, *FloatExpr, *BinaryOpExpr, *FieldAccessExpr:
	default:
		return NewSyntaxError(e.Right.GetPos(), "%s operator with invalid right expression", op)
	}

	if numKeyFieldExpr == 2 || numValueFieldExpr == 2 {
		return NewSyntaxError(e.GetPos(), "%s operator with two same field", op)
	}

	ltype := e.Left.ReturnType()
	rtype := e.Right.ReturnType()
	if ltype != rtype {
		return NewSyntaxError(e.GetPos(), "%s operator left and right type not same", op)
	}
	switch e.Op {
	case Gt, Gte, Lt, Lte:
		if ltype != TNUMBER && ltype != TSTR {
			return NewSyntaxError(e.Left.GetPos(), "%s operator has wrong type of left expression", op)
		}
	case PrefixMatch, RegExpMatch:
		if ltype != TSTR {
			return NewSyntaxError(e.Left.GetPos(), "%s operator has wrong type of left expression", op)
		}
	}
	return nil
}

func (e *BinaryOpExpr) checkWithIn() error {
	ltype := e.Left.ReturnType()
	switch r := e.Right.(type) {
	case *ListExpr:
		for _, expr := range r.List {
			if expr.ReturnType() != ltype {
				return NewSyntaxError(expr.GetPos(), "in operator element has wrong type")
			}
		}
	default:
		return NewSyntaxError(e.Right.GetPos(), "in operator right expression must be list expression")
	}
	return nil
}

func (e *BinaryOpExpr) checkWithBetween() error {
	ltype := e.Left.ReturnType()
	rlist, ok := e.Right.(*ListExpr)
	if !ok || len(rlist.List) != 2 {
		return NewSyntaxError(e.Right.GetPos(), "between operator invalid right expression")
	}

	switch ltype {
	case TSTR, TNUMBER:
	default:
		return NewSyntaxError(e.Left.GetPos(), "between operator only support string and number type")
	}

	lexpr := rlist.List[0]
	uexpr := rlist.List[1]
	if lexpr.ReturnType() != ltype || uexpr.ReturnType() != ltype {
		return NewSyntaxError(e.Right.GetPos(), "between operator right expression with wrong type")
	}
	return nil
}

func (e *FieldExpr) Check() error {
	return nil
}

func (e *StringExpr) Check() error {
	return nil
}

func (e *NotExpr) Check() error {
	if e.Right.ReturnType() != TBOOL {
		return NewSyntaxError(e.Right.GetPos(), "! operator right expression has wrong type")
	}
	return nil
}

func (e *FunctionCallExpr) Check() error {
	_, ok := e.Name.(*NameExpr)
	if !ok {
		return NewSyntaxError(e.Name.GetPos(), "Invalid function name")
	}
	if len(e.Args) > 0 {
		for _, a := range e.Args {
			if err := a.Check(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *NameExpr) Check() error {
	return nil
}

func (e *FloatExpr) Check() error {
	return nil
}

func (e *NumberExpr) Check() error {
	return nil
}

func (e *BoolExpr) Check() error {
	return nil
}

func (e *ListExpr) Check() error {
	if len(e.List) == 0 {
		return NewSyntaxError(e.GetPos(), "Empty list")
	}
	if len(e.List) > 1 {
		ftype := e.List[0].ReturnType()
		for i, item := range e.List[1:] {
			if item.ReturnType() != ftype {
				return NewSyntaxError(item.GetPos(), "List %d item has wrong type", i)
			}
		}
	}
	return nil
}

func (e *FieldAccessExpr) Check() error {
	_, leftIsFAE := e.Left.(*FieldAccessExpr)
	lrType := e.Left.ReturnType()
	switch lrType {
	case TJSON, TLIST:
	default:
		if leftIsFAE {
			// Support cascade field access such as:
			// json(value)['x']['y']
			return nil
		}
		return NewSyntaxError(e.Left.GetPos(), "Field access expression left require JSON or List type")
	}
	switch e.FieldName.(type) {
	case *StringExpr:
		if lrType == TJSON {
			return nil
		} else if leftIsFAE {
			// Support cascade array index access such as:
			// json(value)['list'][1]
			return nil
		}
	case *NumberExpr:
		if lrType == TLIST {
			return nil
		} else if leftIsFAE {
			// Support cascade array index access such as:
			// json(value)['list'][1]
			return nil
		}
	}
	return NewSyntaxError(e.FieldName.GetPos(), "Invalid field name")
}
