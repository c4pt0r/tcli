package utils

import (
	"testing"
)

func TestCmdLine(t *testing.T) {
	l := NewCmdLine([]byte("你好 hello world \"\\x65t   est\""))
	l.Parse()
	if l.Len() != 4 {
		t.Fail()
	}
	if string(l.Args(1)) != "hello" {
		t.Fail()
	}
}
