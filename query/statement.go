package query

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
