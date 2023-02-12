package query

import (
	"fmt"
	"testing"
)

func TestExec1(t *testing.T) {
	query := "where key = 'test' & value = 'x'"
	_, exec, err := BuildExecutor(query)
	if err != nil {
		t.Fatal(err)
	}
	kv := NewKVPStr("test", "x")
	ok, err := exec.Filter(kv)
	if err != nil || !ok {
		t.Fatal(err)
	}
	fmt.Println(ok)

	kv = NewKVPStr("test", "z")
	ok, err = exec.Filter(kv)
	if err != nil || ok {
		t.Fatal(err)
	}
	fmt.Println(ok)
}

func TestExec2(t *testing.T) {
	query := "where key ^= 'test' & value ^= 'z'"
	kvs := []KVPair{
		NewKVPStr("test1", "z1"),
		NewKVPStr("test2", "z2"),
		NewKVPStr("test3", "z3"),
		NewKVPStr("test4", "x1"),
	}
	_, exec, err := BuildExecutor(query)
	if err != nil {
		t.Fatal(err)
	}
	ret, err := exec.FilterBatch(kvs)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(ret)
}

func TestExec3(t *testing.T) {
	query := "where (key = 'test1' | key = 'test4') & value ^= 'z'"
	kvs := []KVPair{
		NewKVPStr("test1", "z1"),
		NewKVPStr("test2", "z2"),
		NewKVPStr("test3", "z3"),
		NewKVPStr("test4", "x1"),
	}
	_, exec, err := BuildExecutor(query)
	if err != nil {
		t.Fatal(err)
	}
	ret, err := exec.FilterBatch(kvs)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(ret)
}

func TestExec4(t *testing.T) {
	query := "where key != 'test1' & value ^= 'z'"
	kvs := []KVPair{
		NewKVPStr("test1", "z1"),
		NewKVPStr("test2", "z2"),
		NewKVPStr("test3", "z3"),
		NewKVPStr("test4", "x1"),
	}
	_, exec, err := BuildExecutor(query)
	if err != nil {
		t.Fatal(err)
	}
	ret, err := exec.FilterBatch(kvs)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(ret)
}
