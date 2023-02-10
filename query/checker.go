package query

import (
	"errors"
	"fmt"
)

func (e *BinaryOpExpr) Check() error {
	switch e.Op {
	case And, Or:
		return e.checkWithAndOr()
	case Not:
		return errors.New("Syntax Error: Invalid operator !")
	case Add, Sub, Mul, Div:
		return e.checkWithMath()
	default:
		return e.checkWithCompares()
	}
}

func (e *BinaryOpExpr) checkWithAndOr() error {
	op := OperatorToString[e.Op]
	switch exp := e.Left.(type) {
	case *BinaryOpExpr, *FunctionCallExpr, *NotExpr:
		if e.Left.ReturnType() != TBOOL {
			return fmt.Errorf("Syntax Error: %s operator has wrong type of left expression %s", op, exp)
		}
	default:
		return fmt.Errorf("Syntax Error: %s operator with invalid left expression %s", op, exp)
	}

	switch exp := e.Right.(type) {
	case *BinaryOpExpr, *FunctionCallExpr, *NotExpr:
		if exp.ReturnType() != TBOOL {
			return fmt.Errorf("Syntax Error: %s operator has wrong type of right expression %s", op, exp)
		}
	default:
		return fmt.Errorf("Syntax Error: %s operator with invalid right expression %s", op, exp)
	}
	return nil
}

func (e *BinaryOpExpr) checkWithMath() error {
	op := OperatorToString[e.Op]
	switch exp := e.Left.(type) {
	case *BinaryOpExpr, *FunctionCallExpr, *NumberExpr, *FloatExpr:
		if e.Left.ReturnType() != TNUMBER {
			return fmt.Errorf("Syntax Error: %s operator has wrong type of left expression %s", op, exp)
		}
	default:
		return fmt.Errorf("Syntax Error: %s operator with invalid left expression %s", op, exp)
	}

	switch exp := e.Right.(type) {
	case *BinaryOpExpr, *FunctionCallExpr, *NumberExpr, *FloatExpr:
		if e.Right.ReturnType() != TNUMBER {
			return fmt.Errorf("Syntax Error: %s operator has wrong type of right expression %s", op, exp)
		}
	default:
		return fmt.Errorf("Syntax Error: %s operator with invalid right expression %s", op, exp)
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
	case *StringExpr, *BoolExpr, *NumberExpr, *FloatExpr:
	default:
		return fmt.Errorf("Syntax Error: %s operator with invalid left expression", op)
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
	case *StringExpr, *BoolExpr, *NumberExpr, *FloatExpr:
	default:
		return fmt.Errorf("Syntax Error: %s operator with invalid right expression", op)
	}

	if numKeyFieldExpr == 2 || numValueFieldExpr == 2 {
		return fmt.Errorf("Syntax Error: %s operator with two same field", op)
	}
	if numKeyFieldExpr == 0 && numValueFieldExpr == 0 && numCallExpr == 0 {
		return fmt.Errorf("Syntax Error: %s operator with no field nor function call", op)
	}

	ltype := e.Left.ReturnType()
	rtype := e.Right.ReturnType()
	if ltype != rtype {
		return fmt.Errorf("Syntax Error: %s operator left and right type not same", op)
	}
	switch e.Op {
	case Gt, Gte, Lt, Lte:
		if ltype != TNUMBER && ltype != TSTR {
			return fmt.Errorf("Syntax Error: %s operator has wrong type of left expression", op)
		}
	case PrefixMatch, RegExpMatch:
		if ltype != TSTR {
			return fmt.Errorf("Syntax Error: %s operator has wrong type of left expression", op)
		}
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
		return errors.New("Syntax Error: ! operator followed wrong type expression")
	}
	return nil
}

func (e *FunctionCallExpr) Check() error {
	_, ok := e.Name.(*NameExpr)
	if !ok {
		return errors.New("Syntax Error: Invalid function name")
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
