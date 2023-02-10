package query

import (
	"errors"
	"fmt"
)

var (
	ErrSyntaxStartWhere    = errors.New("Syntax Error: not start with `where`")
	ErrSyntaxEmptyFields   = errors.New("Syntax Error: empty select fields")
	ErrSyntaxInvalidFields = errors.New("Syntax Error: invalid fields")
)

const MaxNestLevel = 1e5

type Parser struct {
	Query   string
	lex     *Lexer
	toks    []*Token
	tok     *Token
	pos     int
	numToks int
	nestLev int
	exprLev int
}

func NewParser(query string) *Parser {
	lex := NewLexer(query)
	toks := lex.Split()
	return &Parser{
		Query:   query,
		lex:     lex,
		toks:    toks,
		pos:     0,
		numToks: len(toks),
		nestLev: 0,
		exprLev: 0,
	}
}

func (p *Parser) incNestLev() error {
	p.nestLev++
	if p.nestLev > MaxNestLevel {
		return errors.New("exceed max nesting depth")
	}
	return nil
}

func (p *Parser) decNestLev() {
	p.nestLev--
}

func (p *Parser) next() *Token {
	if p.pos >= p.numToks {
		p.tok = nil
		return nil
	}
	p.tok = p.toks[p.pos]
	p.pos += 1
	return p.tok
}

func (p *Parser) expect(tok *Token) error {
	if p.tok == nil {
		return fmt.Errorf("Expect token %s but got EOF", tok.Data)
	}
	if p.tok.Tp != tok.Tp {
		return fmt.Errorf("Expect token %s but got %s", tok.Data, p.tok.Data)
	}
	p.next()
	return nil
}

func (p *Parser) tokPrec() (*Token, int) {
	tok := p.tok
	if tok == nil {
		return nil, LowestPrec
	}
	return tok, tok.Precedence()
}

func (p *Parser) expectOp() (*Token, error) {
	if p.tok == nil {
		return nil, nil
	}
	tp := p.tok.Tp
	switch tp {
	case OPERATOR:
		return p.tok, nil
	}
	return nil, errors.New("Expect operator but got not operator")
}

func (p *Parser) parseExpr() (Expression, error) {
	return p.parseBinaryExpr(nil, LowestPrec+1)
}

func (p *Parser) parseBinaryExpr(x Expression, prec1 int) (Expression, error) {
	var err error
	if x == nil {
		x, err = p.parseUnaryExpr()
		if err != nil {
			return nil, err
		}
	}
	var n int
	defer func() {
		p.nestLev -= n
	}()
	for n = 1; ; n++ {
		err = p.incNestLev()
		if err != nil {
			return nil, err
		}

		opTok, oprec := p.tokPrec()
		if oprec < prec1 {
			return x, nil
		}
		if opTok == nil {
			return x, nil
		}
		err = p.expect(opTok)
		if err != nil {
			return nil, err
		}
		y, err := p.parseBinaryExpr(nil, oprec+1)
		if err != nil {
			return nil, err
		}
		op, err := BuildOp(opTok.Data)
		if err != nil {
			return nil, err
		}
		x = &BinaryOpExpr{Op: op, Left: x, Right: y}
	}
}

func (p *Parser) parseUnaryExpr() (Expression, error) {
	p.incNestLev()
	defer func() {
		p.decNestLev()
	}()
	switch p.tok.Tp {
	case OPERATOR:
		switch p.tok.Data {
		case "!":
			p.next()
			x, err := p.parseUnaryExpr()
			if err != nil {
				return nil, err
			}
			return &NotExpr{Right: x}, nil
		}
	}
	return p.parsePrimaryExpr(nil)
}

func (p *Parser) parseFuncCall(fun Expression) (Expression, error) {
	err := p.expect(&Token{Tp: LPAREN, Data: "("})
	if err != nil {
		return nil, err
	}
	p.exprLev++
	var list []Expression
	for p.tok != nil && p.tok.Tp != RPAREN {
		arg, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		list = append(list, arg)
		if p.tok != nil && p.tok.Tp == RPAREN {
			break
		}
		p.next()
	}
	p.exprLev--
	err = p.expect(&Token{Tp: RPAREN, Data: ")"})
	if err != nil {
		return nil, err
	}
	return &FunctionCallExpr{Name: fun, Args: list}, nil
}

func (p *Parser) parsePrimaryExpr(x Expression) (Expression, error) {
	var err error
	if x == nil {
		x, err = p.parseOperand()
		if err != nil {
			return nil, err
		}
	}

	var n int
	defer func() {
		p.nestLev -= n
	}()

	for n = 1; ; n++ {
		p.incNestLev()
		if p.tok == nil {
			return x, nil
		}
		switch p.tok.Tp {
		case LPAREN:
			x, err = p.parseFuncCall(x)
			if err != nil {
				return nil, err
			}
		default:
			return x, nil
		}
	}
}

func (p *Parser) parseOperand() (Expression, error) {
	switch p.tok.Tp {
	case KEY:
		x := &FieldExpr{Field: KeyKW}
		p.next()
		return x, nil
	case VALUE:
		x := &FieldExpr{Field: ValueKW}
		p.next()
		return x, nil
	case STRING:
		x := &StringExpr{Data: p.tok.Data}
		p.next()
		return x, nil
	case LPAREN:
		p.next()
		p.exprLev++
		x, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		p.exprLev--
		err = p.expect(&Token{Tp: RPAREN, Data: ")"})
		if err != nil {
			return nil, err
		}
		return x, nil
	case NAME:
		x := &NameExpr{Data: p.tok.Data}
		p.next()
		return x, nil
	case NUMBER:
		x := newNumberExpr(p.tok.Data)
		p.next()
		return x, nil
	case FLOAT:
		x := newFloatExpr(p.tok.Data)
		p.next()
		return x, nil
	case TRUE:
		x := &BoolExpr{Data: p.tok.Data, Bool: true}
		p.next()
		return x, nil
	case FALSE:
		x := &BoolExpr{Data: p.tok.Data, Bool: false}
		p.next()
		return x, nil
	}
	return nil, errors.New("Bad Expression")
}

func (p *Parser) parseSelect() (*SelectStmt, error) {
	var (
		fields    = []Expression{}
		allFields = false
		err       error
	)
	err = p.expect(&Token{Tp: SELECT, Data: "select"})
	if err != nil {
		return nil, err
	}
	p.exprLev++
	for p.tok != nil && p.tok.Tp != WHERE {
		if p.tok.Tp == OPERATOR && p.tok.Data == "*" {
			allFields = true
			p.next()
			if p.tok != nil && p.tok.Tp != WHERE {
				return nil, ErrSyntaxInvalidFields
			}
			if len(fields) > 0 {
				return nil, ErrSyntaxInvalidFields
			}
			break
		}
		field, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		fields = append(fields, field)
		if p.tok != nil && p.tok.Tp == WHERE {
			break
		}
		p.next()
	}
	p.exprLev--
	err = p.expect(&Token{Tp: WHERE, Data: "where"})
	if err != nil {
		return nil, err
	}
	if len(fields) == 0 && !allFields {
		return nil, ErrSyntaxEmptyFields
	}

	return &SelectStmt{
		Fields:    fields,
		AllFields: allFields,
	}, nil
}

func (p *Parser) parseLimit() (*LimitStmt, error) {
	var (
		err         error
		shouldBreak bool          = false
		exprs       []*NumberExpr = make([]*NumberExpr, 0, 2)
		ret         *LimitStmt    = &LimitStmt{}
	)
	err = p.expect(&Token{Tp: LIMIT, Data: "limit"})
	if err != nil {
		return nil, err
	}
	for p.tok != nil && !shouldBreak {
		switch p.tok.Tp {
		case NUMBER:
			x := newNumberExpr(p.tok.Data)
			p.next()
			exprs = append(exprs, x)
		case SEP:
			p.next()
			if p.tok == nil || p.tok.Tp != NUMBER {
				return nil, errors.New("Invalid limit parameters after seperator")
			}
		default:
			shouldBreak = true
		}
	}
	if len(exprs) > 2 {
		return nil, errors.New("Too many limit parameters")
	}
	switch len(exprs) {
	case 0:
		return nil, errors.New("Invalid limit parameters")
	case 1:
		ret.Count = int(exprs[0].Int)
	case 2:
		ret.Start = int(exprs[0].Int)
		ret.Count = int(exprs[1].Int)
	}
	return ret, nil
}

func (p *Parser) parseOrderBy() (*OrderStmt, error) {
	var (
		err         error
		shouldBreak bool         = false
		fields      []OrderField = make([]OrderField, 0, 2)
		ret         *OrderStmt   = &OrderStmt{}
	)
	err = p.expect(&Token{Tp: ORDER, Data: "order"})
	if err != nil {
		return nil, err
	}
	err = p.expect(&Token{Tp: BY, Data: "by"})
	if err != nil {
		return nil, err
	}
	p.exprLev++
	for p.tok != nil && !shouldBreak {
		field, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		switch field.ReturnType() {
		case TSTR, TNUMBER, TBOOL:
			break
		default:
			return nil, errors.New("Syntax error: order by field is wrong type.")
		}
		of := OrderField{
			Field: field,
			Order: ASC,
		}
		if p.tok != nil {
			switch p.tok.Tp {
			case SEP:
				p.next()
			case ASC:
				of.Order = ASC
				p.next()
				if p.tok != nil && p.tok.Tp == SEP {
					p.next()
				} else {
					shouldBreak = true
				}
			case DESC:
				of.Order = DESC
				p.next()
				if p.tok != nil && p.tok.Tp == SEP {
					p.next()
				} else {
					shouldBreak = true
				}
			default:
				shouldBreak = true
			}
		}
		fields = append(fields, of)
	}
	p.exprLev--
	ret.Orders = fields
	return ret, nil
}

func (p *Parser) Parse() (*SelectStmt, error) {
	if p.numToks == 0 {
		return nil, ErrSyntaxStartWhere
	}
	p.next()
	if p.tok == nil || (p.tok.Tp != WHERE && p.tok.Tp != SELECT) {
		return nil, ErrSyntaxStartWhere
	}
	var (
		selectStmt *SelectStmt = nil
		limitStmt  *LimitStmt  = nil
		orderStmt  *OrderStmt  = nil
		err        error
	)

	if p.tok.Tp == SELECT {
		selectStmt, err = p.parseSelect()
		if err != nil {
			return nil, err
		}
	} else {
		if p.tok.Tp != WHERE {
			return nil, ErrSyntaxStartWhere
		}
		p.next()
	}

	expr, err := p.parseExpr()
	if err != nil {
		return nil, err
	}

	for p.tok != nil {
		switch p.tok.Tp {
		case ORDER:
			if orderStmt != nil {
				return nil, errors.New("Syntax error duplicate order by expression")
			}
			orderStmt, err = p.parseOrderBy()
			if err != nil {
				return nil, err
			}
		case LIMIT:
			if limitStmt != nil {
				return nil, errors.New("Syntax error duplicate limit expression")
			}
			limitStmt, err = p.parseLimit()
			if err != nil {
				return nil, err
			}
			if p.tok != nil {
				return nil, errors.New("Syntax error has more expression after limit keyword")
			}
		default:
			return nil, errors.New("Syntax error missing operator")
		}
	}

	// Check syntax
	err = expr.Check()
	if err != nil {
		return nil, err
	}
	if expr.ReturnType() != TBOOL {
		return nil, errors.New("Syntax error where should follow bool result expression")
	}
	whereStmt := &WhereStmt{
		Expr: expr,
	}
	if selectStmt == nil {
		selectStmt = &SelectStmt{
			Fields:    nil,
			AllFields: true,
		}
	}
	selectStmt.Where = whereStmt
	selectStmt.Limit = limitStmt
	selectStmt.Order = orderStmt
	return selectStmt, nil
}
