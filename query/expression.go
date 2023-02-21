package query

import (
	"fmt"
	"strconv"
	"strings"
)

/*
Query Examples:
	query 'where key ^= "test"'        // key prefix match
	query 'where key ~= "[regexp]"'    // key regexp match
	query 'where value ^= "test"'      // value prefix match
	query 'where value ~= "[regexp]"'  // value regexp match
*/

type KVKeyword byte
type Operator byte
type Type byte

const (
	KeyKW   KVKeyword = 1
	ValueKW KVKeyword = 2

	Unknown     Operator = 0
	And         Operator = 1
	Or          Operator = 2
	Not         Operator = 3
	Eq          Operator = 4
	NotEq       Operator = 5
	PrefixMatch Operator = 6
	RegExpMatch Operator = 7
	Add         Operator = 8
	Sub         Operator = 9
	Mul         Operator = 10
	Div         Operator = 11
	Gt          Operator = 12
	Gte         Operator = 13
	Lt          Operator = 14
	Lte         Operator = 15
	In          Operator = 16
	Between     Operator = 17

	TUNKNOWN Type = 0
	TBOOL    Type = 1
	TSTR     Type = 2
	TNUMBER  Type = 3
	TIDENT   Type = 4
	TLIST    Type = 5
)

var (
	KVKeywordToString = map[KVKeyword]string{
		KeyKW:   "KEY",
		ValueKW: "VALUE",
	}

	OperatorToString = map[Operator]string{
		Eq:          "=",
		NotEq:       "!=",
		And:         "&",
		Or:          "|",
		Not:         "!",
		PrefixMatch: "^=",
		RegExpMatch: "~=",
		Add:         "+",
		Sub:         "-",
		Mul:         "*",
		Div:         "/",
		Gt:          ">",
		Gte:         ">=",
		Lt:          "<",
		Lte:         "<=",
		In:          "in",
		Between:     "between",
	}

	StringToOperator = map[string]Operator{
		"=":       Eq,
		"&":       And,
		"|":       Or,
		"!":       Not,
		"^=":      PrefixMatch,
		"~=":      RegExpMatch,
		"!=":      NotEq,
		"+":       Add,
		"-":       Sub,
		"*":       Mul,
		"/":       Div,
		">":       Gt,
		">=":      Gte,
		"<":       Lt,
		"<=":      Lte,
		"in":      In,
		"between": Between,
	}
)

func BuildOp(op string) (Operator, error) {
	ret, have := StringToOperator[op]
	if !have {
		return Unknown, ErrSyntaxUnknownOperator
	}
	return ret, nil
}

/*
query: where key ^= "test" & value ~= "test"
WhereStmt {
	Expr: BinaryOpExpr {
		Op: "&",
		Left: BinaryOpExpr {
			Op: "^=",
			Left: FieldExpr{Field: KEY},
			Right: StringExpr{Data: "test"},
		},
		Right: BinaryOpExpr {
			Op: "~=",
			Left: FieldExpr{Field: VALUE},
			Right: StringExpr{Data: "test"},
		}
	},
}
*/

var (
	_ Expression = (*BinaryOpExpr)(nil)
	_ Expression = (*FieldExpr)(nil)
	_ Expression = (*StringExpr)(nil)
	_ Expression = (*NotExpr)(nil)
	_ Expression = (*FunctionCallExpr)(nil)
	_ Expression = (*NameExpr)(nil)
	_ Expression = (*NumberExpr)(nil)
	_ Expression = (*FloatExpr)(nil)
	_ Expression = (*BoolExpr)(nil)
	_ Expression = (*ListExpr)(nil)
)

type Expression interface {
	Check() error
	String() string
	Execute(kv KVPair) (any, error)
	ReturnType() Type
}

type BinaryOpExpr struct {
	Op    Operator
	Left  Expression
	Right Expression
}

func (e *BinaryOpExpr) String() string {
	op := OperatorToString[e.Op]
	switch op {
	case "between":
		list, ok := e.Right.(*ListExpr)
		if !ok || len(list.List) != 2 {
			return fmt.Sprintf("(%s %s %s)", e.Left.String(), op, e.Right.String())
		}
		return fmt.Sprintf("(%s BETWEEN %s AND %s)", e.Left.String(), list.List[0].String(), list.List[1].String())
	default:
		return fmt.Sprintf("(%s %s %s)", e.Left.String(), op, e.Right.String())
	}
}

func (e *BinaryOpExpr) ReturnType() Type {
	switch e.Op {
	case And, Or, Not, Eq, NotEq, PrefixMatch, RegExpMatch, Gt, Gte, Lt, Lte, In, Between:
		return TBOOL
	case Sub, Mul, Div:
		return TNUMBER
	case Add:
		if e.Left.ReturnType() == TSTR {
			return TSTR
		}
		return TNUMBER
	}
	return TUNKNOWN
}

type FieldExpr struct {
	Field KVKeyword
}

func (e *FieldExpr) String() string {
	return fmt.Sprintf("%s", KVKeywordToString[e.Field])
}

func (e *FieldExpr) ReturnType() Type {
	return TSTR
}

type StringExpr struct {
	Data string
}

func (e *StringExpr) String() string {
	return fmt.Sprintf("`%s`", e.Data)
}

func (e *StringExpr) ReturnType() Type {
	return TSTR
}

type NotExpr struct {
	Right Expression
}

func (e *NotExpr) String() string {
	return fmt.Sprintf("!(%s)", e.Right.String())
}

func (e *NotExpr) ReturnType() Type {
	return TBOOL
}

type FunctionCallExpr struct {
	Name   Expression
	Args   []Expression
	Result any
}

func (e *FunctionCallExpr) String() string {
	args := make([]string, len(e.Args))
	for i, expr := range e.Args {
		args[i] = expr.String()
	}
	return fmt.Sprintf("%s(%s)", e.Name.String(), strings.Join(args, ", "))
}

func (e *FunctionCallExpr) ReturnType() Type {
	fname, err := GetFuncNameFromExpr(e)
	if err != nil {
		return TUNKNOWN
	}

	if funcObj, have := GetScalarFunctionByName(fname); have {
		return funcObj.ReturnType
	}
	if funcObj, have := GetAggrFunctionByName(fname); have {
		return funcObj.ReturnType
	}
	return TUNKNOWN
}

type NameExpr struct {
	Data string
}

func (e *NameExpr) String() string {
	return fmt.Sprintf("%s", e.Data)
}

func (e *NameExpr) ReturnType() Type {
	return TIDENT
}

type NumberExpr struct {
	Data string
	Int  int64
}

func newNumberExpr(data string) *NumberExpr {
	num, err := strconv.ParseInt(data, 10, 64)
	if err != nil {
		num = 0
	}
	return &NumberExpr{
		Data: data,
		Int:  num,
	}
}

func (e *NumberExpr) String() string {
	return fmt.Sprintf("%s", e.Data)
}

func (e *NumberExpr) ReturnType() Type {
	return TNUMBER
}

type FloatExpr struct {
	Data  string
	Float float64
}

func newFloatExpr(data string) *FloatExpr {
	num, err := strconv.ParseFloat(data, 64)
	if err != nil {
		num = 0.0
	}
	return &FloatExpr{
		Data:  data,
		Float: num,
	}
}

func (e *FloatExpr) String() string {
	return fmt.Sprintf("%s", e.Data)
}

func (e *FloatExpr) ReturnType() Type {
	return TNUMBER
}

type BoolExpr struct {
	Data string
	Bool bool
}

func (e *BoolExpr) String() string {
	return fmt.Sprintf("%s", e.Data)
}

func (e *BoolExpr) ReturnType() Type {
	return TBOOL
}

type ListExpr struct {
	List []Expression
}

func (e *ListExpr) String() string {
	ret := make([]string, len(e.List))
	for i, item := range e.List {
		ret[i] = item.String()
	}
	return fmt.Sprintf("(%s)", strings.Join(ret, ", "))
}

func (e *ListExpr) ReturnType() Type {
	return TLIST
}
