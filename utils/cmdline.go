package utils

import (
	"errors"
	"fmt"
	"io"
	"unicode"
	"unicode/utf8"
)

func isLetter(ch rune) bool {
	return 'a' <= lower(ch) && lower(ch) <= 'z' || ch == '_' || ch >= utf8.RuneSelf && unicode.IsLetter(ch)
}
func lower(ch rune) rune     { return ('a' - 'A') | ch } // returns lower-case ch iff ch is ASCII letter
func isDecimal(ch rune) bool { return '0' <= ch && ch <= '9' }
func isHex(ch rune) bool     { return '0' <= ch && ch <= '9' || 'a' <= lower(ch) && lower(ch) <= 'f' }

func isDigit(ch rune) bool {
	return isDecimal(ch) || ch >= utf8.RuneSelf && unicode.IsDigit(ch)
}

func digitVal(ch rune) int {
	switch {
	case '0' <= ch && ch <= '9':
		return int(ch - '0')
	case 'a' <= lower(ch) && lower(ch) <= 'f':
		return int(lower(ch) - 'a' + 10)
	}
	return 16 // larger than any legal digit val
}

type CmdLine struct {
	raw []byte
	// parsed arguments, escaped
	args [][]byte
	// raw arguments
	rawArgs [][]byte

	offset   int // current offset
	rdOffset int // next offset
	ch       rune
}

func NewCmdLine(raw []byte) *CmdLine {
	return &CmdLine{
		raw:      raw,
		ch:       rune(raw[0]),
		offset:   0,
		rdOffset: 0,
	}
}

func (l *CmdLine) Parse() error {
	for l.ch != -1 {
		arg, err := l.scan()
		if err != nil {
			return err
		}
		if arg == nil {
			return nil
		}
		l.args = append(l.args, arg)
	}
	return nil
}

func (l *CmdLine) scan() ([]byte, error) {
	err := l.skipWhitespace()
	if err != nil {
		return nil, err
	}
	switch ch := l.ch; {
	case isLetter(ch):
		arg, err := l.scanNormalArgv()
		if err != nil {
			return nil, err
		}
		return arg, nil
	default:
		l.next() // always make progress
		switch ch {
		case -1:
			return nil, io.EOF
		case '"':
			arg, err := l.scanString()
			if err != nil {
				return nil, err
			}
			return arg, nil
		}
	}
	return nil, nil
}

func (l *CmdLine) scanString() ([]byte, error) {
	offs := l.offset - 1

	for {
		ch := l.ch
		if ch == '\n' || ch < 0 {
			return nil, l.error(offs, "string literal not terminated")
		}
		l.next()
		if ch == '"' {
			break
		}
		if ch == '\\' {
			l.scanEscape('"')
		}
	}
	s := string(l.raw[offs:l.offset])
	return []byte(s), nil
}

func (l *CmdLine) scanEscape(quote rune) (bool, error) {
	offs := l.rdOffset
	var n int
	var base, max uint32
	switch l.ch {
	case 'a', 'b', 'f', 'n', 'r', 't', 'v', '\\', quote:
		l.next()
		return true, nil
	case '0', '1', '2', '3', '4', '5', '6', '7':
		n, base, max = 3, 8, 255
	case 'x':
		l.next()
		n, base, max = 2, 16, 255
	case 'u':
		l.next()
		n, base, max = 4, 16, unicode.MaxRune
	case 'U':
		l.next()
		n, base, max = 8, 16, unicode.MaxRune
	default:
		msg := "unknown escape sequence"
		if l.ch < 0 {
			msg = "escape sequence not terminated"
		}
		return false, l.error(offs, msg)
	}

	var x uint32
	for n > 0 {
		d := uint32(digitVal(l.ch))
		if d >= base {
			msg := fmt.Sprintf("illegal character %#U in escape sequence", l.ch)
			if l.ch < 0 {
				msg = "escape sequence not terminated"
			}
			return false, l.error(l.rdOffset, msg)
		}
		x = x*base + d
		l.next()
		n--
	}

	if x > max || 0xD800 <= x && x < 0xE000 {
		return false, l.error(offs, "escape sequence is invalid Unicode code point")
	}

	return true, nil
}

func (l *CmdLine) scanNormalArgv() ([]byte, error) {
	offs := l.offset
	for isLetter(l.ch) || isDigit(l.ch) {
		if err := l.next(); err != nil {
			return nil, err
		}
	}
	buf := string(l.raw[offs:l.offset])
	return []byte(buf), nil
}

func (l *CmdLine) next() error {
	if l.rdOffset < len(l.raw) {
		l.offset = l.rdOffset
		r, w := rune(l.raw[l.rdOffset]), 1
		switch {
		case r == 0:
			return l.error(l.rdOffset, "illegal character NUL")
		case r >= utf8.RuneSelf:
			r, w = utf8.DecodeRune(l.raw[l.rdOffset:])
			if r == utf8.RuneError && w == 1 {
				return l.error(l.rdOffset, "illegal UTF-8 encoding")
			}
		}
		l.rdOffset += w
		l.ch = r
	} else {
		l.offset = len(l.raw)
		l.ch = -1
	}
	return nil
}

func (l *CmdLine) skipWhitespace() error {
	for l.ch == ' ' || l.ch == '\t' {
		if err := l.next(); err != nil {
			return err
		}
	}
	return nil
}

func (l *CmdLine) error(offset int, msg string) error {
	return errors.New(fmt.Sprintf("%s, offset: %d", msg, offset))
}

func (l *CmdLine) Len() int {
	return len(l.args)
}

func (l *CmdLine) Args(i int) []byte {
	if i >= 0 && i < len(l.args) {
		return l.args[i]
	}
	return nil
}

func (l *CmdLine) RawArgs(i int) []string {
	panic("not implemented")
}
