package query

import "errors"

type SelectStmt struct {
	AllFields  bool
	FieldNames []string
	FieldTypes []Type
	Fields     []Expression
	Where      *WhereStmt
	Order      *OrderStmt
	Limit      *LimitStmt
	GroupBy    *GroupByStmt
}

type WhereStmt struct {
	Expr Expression
}

type OrderField struct {
	Name  string
	Field Expression
	Order TokenType
}

type OrderStmt struct {
	Orders []OrderField
}

type GroupByField struct {
	Name string
	Expr Expression
}

type GroupByStmt struct {
	Fields []GroupByField
}

type LimitStmt struct {
	Start int
	Count int
}

func (s *SelectStmt) ValidateFields() error {
	for _, f := range s.Fields {
		if err := s.validateField(f); err != nil {
			return err
		}
	}
	return nil
}

func (s *SelectStmt) validateField(f Expression) error {
	if err := f.Check(); err != nil {
		return err
	}

	return s.checkAggrFunctionArgs(f)
}

func (s *SelectStmt) checkAggrFunctionArgs(expr Expression) error {
	var err error
	switch e := expr.(type) {
	case *BinaryOpExpr:
		err = s.checkAggrFunctionArgs(e.Left)
		if err != nil {
			return err
		}
		err = s.checkAggrFunctionArgs(e.Right)
		if err != nil {
			return err
		}
	case *FunctionCallExpr:
		fname, err := GetFuncNameFromExpr(e)
		if err == nil && IsAggrFunc(fname) {
			err = s.checkAggrFuncArgs(e.Args)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *SelectStmt) checkAggrFuncArgs(args []Expression) error {
	for _, arg := range args {
		if err := s.checkAggrFuncArg(arg); err != nil {
			return err
		}
	}
	return nil
}

func (s *SelectStmt) checkAggrFuncArg(arg Expression) error {
	var err error
	switch e := arg.(type) {
	case *BinaryOpExpr:
		err = s.checkAggrFuncArg(e.Left)
		if err != nil {
			return err
		}
		err = s.checkAggrFuncArg(e.Right)
		if err != nil {
			return err
		}
	case *FunctionCallExpr:
		fname, err := GetFuncNameFromExpr(e)
		if err == nil && IsAggrFunc(fname) {
			return errors.New("Aggregate function arguments should not contains aggregate function")
		}
	}
	return nil
}
