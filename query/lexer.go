package query

import (
	"fmt"
	"strconv"
	"strings"
)

type TokenType byte

const (
	SELECT   TokenType = 1
	WHERE    TokenType = 2
	KEY      TokenType = 3
	VALUE    TokenType = 4
	OPERATOR TokenType = 5
	STRING   TokenType = 6
	LPAREN   TokenType = 7
	RPAREN   TokenType = 8
	NAME     TokenType = 9
	SEP      TokenType = 10
	NUMBER   TokenType = 11
	FLOAT    TokenType = 12
	LIMIT    TokenType = 13
	ORDER    TokenType = 14
	BY       TokenType = 15
	ASC      TokenType = 16
	DESC     TokenType = 17
	TRUE     TokenType = 18
	FALSE    TokenType = 19
)

var (
	TokenTypeToString = map[TokenType]string{
		SELECT:   "SELECT",
		WHERE:    "WHERE",
		KEY:      "KEY",
		VALUE:    "VALUE",
		OPERATOR: "OP",
		STRING:   "STR",
		LPAREN:   "(",
		RPAREN:   ")",
		NAME:     "NAME",
		SEP:      "SEP",
		NUMBER:   "NUM",
		FLOAT:    "FLOAT",
		LIMIT:    "LIMIT",
		ORDER:    "ORDER",
		BY:       "BY",
		ASC:      "ASC",
		DESC:     "DESC",
		TRUE:     "true",
		FALSE:    "false",
	}
)

const (
	LowestPrec  = 0 // non-operators
	UnaryPrec   = 6
	HighestPrec = 7
)

type Token struct {
	Tp   TokenType
	Data string
	Pos  int
}

func (t *Token) String() string {
	tp := TokenTypeToString[t.Tp]
	return fmt.Sprintf("Tp: %6s  Data: %10s  Pos: %d", tp, t.Data, t.Pos)
}

func (t *Token) Precedence() int {
	switch t.Tp {
	case OPERATOR:
		switch t.Data {
		case "|":
			return 1
		case "&":
			return 2
		case "=", "!=", "^=", "~=", ">", ">=", "<", "<=":
			return 3
		case "+", "-":
			return 4
		case "*", "/":
			return 5
		}
	}
	return LowestPrec
}

type Lexer struct {
	Query  string
	Length int
}

func NewLexer(query string) *Lexer {
	return &Lexer{
		Query:  query,
		Length: len(query),
	}
}

func (l *Lexer) Split() []*Token {
	var (
		curr         string
		prev         byte
		next         byte
		ret          []*Token
		strStart     bool = false
		strStartChar byte = 0
		tokStartPos  int
	)
	for i := 0; i < l.Length; i++ {
		char := l.Query[i]
		if i < l.Length-1 {
			next = l.Query[i+1]
		} else {
			next = 0
		}
		switch char {
		case ' ':
			if strStart {
				curr += string(char)
				break
			}
			if token := buildToken(curr, tokStartPos); token != nil {
				ret = append(ret, token)
			}
			curr = ""
			tokStartPos = i + 1
		case '"', '\'':
			if !strStart {
				strStart = true
				strStartChar = char
				tokStartPos = i
			} else if strStartChar == char {
				strStart = false
				token := &Token{
					Tp:   STRING,
					Data: curr,
					Pos:  tokStartPos,
				}
				ret = append(ret, token)
				curr = ""
			} else {
				curr += string(char)
			}
		case '~', '^', '=', '!', '*', '+', '-', '/', '>', '<':
			if strStart {
				curr += string(char)
				break
			}
			if token := buildToken(curr, tokStartPos); token != nil {
				ret = append(ret, token)
			}
			curr = ""
			var token *Token = nil

			if next != '=' {
				switch char {
				case '!', '*', '+', '-', '/':
					token = &Token{
						Tp:   OPERATOR,
						Data: string(char),
						Pos:  i,
					}
				case '>', '<':
					token = &Token{
						Tp:   OPERATOR,
						Data: string(char),
						Pos:  i,
					}
				}
			}
			if token != nil {
				ret = append(ret, token)
				tokStartPos = i + 1
				break
			}

			if char == '=' {
				switch prev {
				case '^':
					token = &Token{
						Tp:   OPERATOR,
						Data: "^=",
						Pos:  i - 1,
					}
				case '~':
					token = &Token{
						Tp:   OPERATOR,
						Data: "~=",
						Pos:  i - 1,
					}
				case '!':
					token = &Token{
						Tp:   OPERATOR,
						Data: "!=",
						Pos:  i - 1,
					}
				case '<':
					token = &Token{
						Tp:   OPERATOR,
						Data: "<=",
						Pos:  i - 1,
					}
				case '>':
					token = &Token{
						Tp:   OPERATOR,
						Data: ">=",
						Pos:  i - 1,
					}
				default:
					token = &Token{
						Tp:   OPERATOR,
						Data: "=",
						Pos:  i,
					}
				}
				if token != nil {
					ret = append(ret, token)
				}
			}
			tokStartPos = i + 1
		case '&', '|', '(', ')':
			if strStart {
				curr += string(char)
				break
			}
			token := buildToken(curr, tokStartPos)
			if token != nil {
				ret = append(ret, token)
			}
			if char == '(' {
				token = &Token{
					Tp:   LPAREN,
					Data: string(char),
					Pos:  i,
				}
			} else if char == ')' {
				token = &Token{
					Tp:   RPAREN,
					Data: string(char),
					Pos:  i,
				}
			} else {
				token = &Token{
					Tp:   OPERATOR,
					Data: string(char),
					Pos:  i,
				}
			}
			ret = append(ret, token)
			curr = ""
			tokStartPos = i + 1
		case ',':
			if strStart {
				curr += string(char)
				break
			}
			token := buildToken(curr, tokStartPos)
			if token != nil {
				ret = append(ret, token)
			}
			token = &Token{
				Tp:   SEP,
				Data: ",",
				Pos:  i,
			}
			ret = append(ret, token)
			curr = ""
			tokStartPos = i + 1
		default:
			curr += string(char)
		}
		prev = char
	}
	if len(curr) > 0 {
		if token := buildToken(curr, tokStartPos); token != nil {
			ret = append(ret, token)
		}
	}
	return ret
}

func isNumber(val string) bool {
	if _, err := strconv.ParseInt(val, 10, 64); err == nil {
		return true
	}
	return false
}

func isFloat(val string) bool {
	if _, err := strconv.ParseFloat(val, 64); err == nil {
		return true
	}
	return false
}

func buildToken(curr string, pos int) *Token {
	curr = strings.ToLower(strings.TrimSpace(curr))
	if len(curr) == 0 {
		return nil
	}
	token := &Token{
		Data: curr,
		Pos:  pos,
	}
	switch curr {
	case "select":
		token.Tp = SELECT
		return token
	case "where":
		token.Tp = WHERE
		return token
	case "key":
		token.Tp = KEY
		return token
	case "value":
		token.Tp = VALUE
		return token
	case "limit":
		token.Tp = LIMIT
		return token
	case "order":
		token.Tp = ORDER
		return token
	case "by":
		token.Tp = BY
		return token
	case "asc":
		token.Tp = ASC
		return token
	case "desc":
		token.Tp = DESC
		return token
	case "true":
		token.Tp = TRUE
		return token
	case "false":
		token.Tp = FALSE
		return token
	default:
		if isNumber(curr) {
			token.Tp = NUMBER
			return token
		} else if isFloat(curr) {
			token.Tp = FLOAT
			return token
		}
		token.Tp = NAME
		return token
	}
	return nil
}
