package utils

import (
	"testing"
)

func TestCmdLine(t *testing.T) {
	l := NewCmdLine([]byte("你好 h@ello world 5.3 !@#$%^&*(*&^%$#@#$%$#) \"\\x65t   est\""))
	l.Parse()
	if l.Len() != 6 {
		t.Fail()
	}
	if string(l.Args(1)) != "h@ello" {
		t.Fail()
	}
}
