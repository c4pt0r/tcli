package query

import "fmt"

type ExpressionOptimizer struct {
	Root   Expression
	parent Expression
}

func (o *ExpressionOptimizer) Optimize() Expression {
	newRoot := o.optimize(o.Root)
	return newRoot
}

func (o *ExpressionOptimizer) optimize(expr Expression) Expression {
	switch e := expr.(type) {
	case *BinaryOpExpr:
		nexpr, _ := o.tryOptimizeBinaryOpExecute(e)
		nexpr, _ = o.tryOptimizeAndOr(nexpr)
		return nexpr
	case *FunctionCallExpr:
		nexpr, _ := o.tryOptimizeFunctionCall(e)
		return nexpr
	}
	return expr
}

func (o *ExpressionOptimizer) tryOptimizeBinaryOpExecute(e *BinaryOpExpr) (Expression, bool) {
	leftIsValue := false
	rightIsValue := false
	switch left := e.Left.(type) {
	case *BinaryOpExpr:
		e.Left, leftIsValue = o.tryOptimizeBinaryOpExecute(left)
	case *FunctionCallExpr:
		e.Left, leftIsValue = o.tryOptimizeFunctionCall(left)
	case *StringExpr, *NumberExpr, *FloatExpr, *BoolExpr:
		leftIsValue = true
	}

	switch right := e.Right.(type) {
	case *BinaryOpExpr:
		e.Right, rightIsValue = o.tryOptimizeBinaryOpExecute(right)
	case *FunctionCallExpr:
		e.Right, rightIsValue = o.tryOptimizeFunctionCall(right)
	case *StringExpr, *NumberExpr, *FloatExpr, *BoolExpr:
		rightIsValue = true
	}
	// Not value
	if !(leftIsValue && rightIsValue) {
		return e, false
	}
	switch e.Op {
	case Add, Sub, Mul, Div:
		ret, err := e.Execute(NewKVP(nil, nil))
		if err == nil {
			switch e.Left.(type) {
			case *StringExpr:
				return &StringExpr{Data: ret.(string)}, true
			case *NumberExpr:
				return &NumberExpr{Data: fmt.Sprintf("%v", ret), Int: ret.(int64)}, true
			case *FloatExpr:
				return &FloatExpr{Data: fmt.Sprintf("%v", ret), Float: ret.(float64)}, true
			}
		}
	case And, Or:
		ret, err := e.Execute(NewKVP(nil, nil))
		if err == nil {
			return &BoolExpr{Data: fmt.Sprintf("%v", ret), Bool: ret.(bool)}, true
		}
	case Eq, NotEq, Gt, Gte, Lt, Lte:
		ret, err := e.Execute(NewKVP(nil, nil))
		if err == nil {
			return &BoolExpr{Data: fmt.Sprintf("%v", ret), Bool: ret.(bool)}, true
		}
	}
	return e, false
}

func (o *ExpressionOptimizer) tryOptimizeAndOr(expr Expression) (Expression, bool) {
	var (
		leftVal      bool
		rightVal     bool
		leftIsValue  = false
		rightIsValue = false
	)

	e, ok := expr.(*BinaryOpExpr)
	if !ok {
		return expr, false
	}
	if e.Op != And && e.Op != Or {
		return e, false
	}
	switch left := e.Left.(type) {
	case *BoolExpr:
		leftIsValue = true
		leftVal = left.Bool
	}

	switch right := e.Right.(type) {
	case *BoolExpr:
		rightIsValue = true
		rightVal = right.Bool
	}

	if leftIsValue && !rightIsValue {
		switch e.Op {
		case And:
			if leftVal {
				// true & Expr => Expr
				return e.Right, true
			} else {
				// false & Expr => false
				return &BoolExpr{Data: "false", Bool: false}, true
			}
		case Or:
			if leftVal {
				// true | Expr => true
				return &BoolExpr{Data: "true", Bool: true}, true
			} else {
				// false | Expr => Expr
				return e.Right, true
			}
		}
	}

	if rightIsValue && !leftIsValue {
		switch e.Op {
		case And:
			if rightVal {
				// Expr & true => Expr
				return e.Left, true
			} else {
				// Expr & false => false
				return &BoolExpr{Data: "false", Bool: false}, true
			}
		case Or:
			if rightVal {
				// Expr | true => true
				return &BoolExpr{Data: "true", Bool: true}, true
			} else {
				// Expr | false => Expr
				return e.Left, true
			}
		}
	}

	if rightIsValue && rightIsValue {
		switch e.Op {
		case And:
			if leftVal && rightVal {
				return &BoolExpr{Data: "true", Bool: true}, true
			}
			return &BoolExpr{Data: "false", Bool: false}, true
		case Or:
			if leftVal || rightVal {
				return &BoolExpr{Data: "true", Bool: true}, true
			}
			return &BoolExpr{Data: "false", Bool: false}, true
		}
	}

	return e, false
}

func (o *ExpressionOptimizer) tryOptimizeFunctionCall(e *FunctionCallExpr) (Expression, bool) {
	allIsValue := true
	for i, arg := range e.Args {
		nexpr := o.optimize(arg)
		e.Args[i] = nexpr
		switch nexpr.(type) {
		case *StringExpr, *NumberExpr, *FloatExpr, *BoolExpr:
			// Value
		default:
			allIsValue = false
		}
	}

	if !(allIsValue && IsScalarFuncExpr(e)) {
		return e, false
	}

	retTp := e.ReturnType()
	ret, err := e.Execute(NewKVP(nil, nil))
	if err == nil {
		switch retTp {
		case TSTR:
			return &StringExpr{Data: ret.(string)}, true
		case TNUMBER:
			iret, ok := ret.(int64)
			if ok {
				return &NumberExpr{Data: fmt.Sprintf("%v", ret), Int: iret}, true
			}
			fret, ok := ret.(float64)
			if ok {
				return &FloatExpr{Data: fmt.Sprintf("%v", ret), Float: fret}, true
			}
		case TBOOL:
			if ret.(bool) {
				return &BoolExpr{Data: "true", Bool: true}, true
			}
			return &BoolExpr{Data: "false", Bool: false}, true
		}
	}
	return e, false
}
