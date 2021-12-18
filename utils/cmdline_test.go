package utils

import (
	"testing"

	"github.com/c4pt0r/log"
)

func TestCmdLine(t *testing.T) {
	l := NewCmdLine([]byte("你好 \"测\\x32试\" 测试\x32测 试  'tes     t@test@test' \"te\\\\xdst\" abcd"))
	err := l.Parse()
	if err != nil {
		log.E(err)
		t.Fail()
	}
	if l.Len() != 4 {
		t.Fail()
	}
}
