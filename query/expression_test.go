package query

import (
	"bytes"
	"fmt"
	"sort"
	"testing"
)

var benchmarkChunkSize = 32

type mockQueryTxn struct {
	data []KVPair
}

func newMockQueryTxn(data []KVPair) *mockQueryTxn {
	sort.Slice(data, func(i, j int) bool {
		return bytes.Compare(data[i].Key, data[j].Key) < 0
	})
	return &mockQueryTxn{
		data: data,
	}
}

func (t *mockQueryTxn) Get(key []byte) ([]byte, error) {
	for _, kvp := range t.data {
		if bytes.Equal(kvp.Key, key) {
			return kvp.Value, nil
		}
	}
	return nil, nil
}

func (t *mockQueryTxn) Cursor() (Cursor, error) {
	return &mockCursor{
		data:   t.data,
		idx:    0,
		length: len(t.data),
	}, nil
}

type mockCursor struct {
	data   []KVPair
	idx    int
	length int
}

func (c *mockCursor) Seek(key []byte) error {
	for c.idx < c.length {
		row := c.data[c.idx]
		if bytes.Compare(row.Key, key) >= 0 {
			break
		}
		c.idx++
	}
	return nil
}

func (c *mockCursor) Next() (key []byte, val []byte, err error) {
	if c.idx >= c.length {
		return nil, nil, nil
	}
	ret := c.data[c.idx]
	c.idx++
	return ret.Key, ret.Value, nil
}

func generateChunk(size int) []KVPair {
	ret := make([]KVPair, size)
	for i := 0; i < size; i++ {
		key := fmt.Sprintf("key-%d", i)
		val := fmt.Sprintf("%d", i)
		ret[i] = NewKVPStr(key, val)
	}
	return ret
}

func BenchmarkExpressionEvalVec(b *testing.B) {
	chunk := generateChunk(benchmarkChunkSize)
	query := "where key ^= 'key-1' & int(value) + int(value) * 8 > 10"
	_, exec, err := BuildExecutor(query)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, err = exec.filterChunk(chunk)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExpressionEval(b *testing.B) {
	chunk := generateChunk(benchmarkChunkSize)
	query := "where key ^= 'key-1' & int(value) + int(value) * 8 > 10"
	_, exec, err := BuildExecutor(query)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for i := 0; i < len(chunk); i++ {
			_, err = exec.Filter(chunk[i])
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkExpressionEvalHalfVec(b *testing.B) {
	chunk := generateChunk(benchmarkChunkSize)
	query := "where key ^= 'key-1' & int(value) + int(value) * 8 > 10"
	_, exec, err := BuildExecutor(query)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, err = exec.filterBatch(chunk)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQuery(b *testing.B) {
	query := "select sum(int(value)) * 2, key + '_' + 'end' as kk, int(value) as ival where key between 'k' and 'l' group by kk, ival order by ival desc"
	data := generateChunk(1000)
	qtxn := newMockQueryTxn(data)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		opt := NewOptimizer(query)
		plan, err := opt.BuildPlan(qtxn)
		if err != nil {
			b.Fatal(err)
		}
		err = getRows(plan)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryBatch(b *testing.B) {
	query := "select sum(int(value)) * 2, key + '_' + 'end' as kk, int(value) as ival where key between 'k' and 'l' group by kk, ival order by ival desc"
	data := generateChunk(1000)
	qtxn := newMockQueryTxn(data)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		opt := NewOptimizer(query)
		plan, err := opt.BuildPlan(qtxn)
		if err != nil {
			b.Fatal(err)
		}
		err = getRowsBatch(plan)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQuerySimple(b *testing.B) {
	query := "select int(value) * 2, key + '_' + 'end' as kk, int(value) as ival where key between 'k' and 'l' limit 100"
	data := generateChunk(1000)
	qtxn := newMockQueryTxn(data)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		opt := NewOptimizer(query)
		plan, err := opt.BuildPlan(qtxn)
		if err != nil {
			b.Fatal(err)
		}
		err = getRows(plan)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQuerySimpleBatch(b *testing.B) {
	query := "select int(value) * 2, key + '_' + 'end' as kk, int(value) as ival where key between 'k' and 'l' limit 100"
	data := generateChunk(1000)
	qtxn := newMockQueryTxn(data)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		opt := NewOptimizer(query)
		plan, err := opt.BuildPlan(qtxn)
		if err != nil {
			b.Fatal(err)
		}
		err = getRowsBatch(plan)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func getRows(plan FinalPlan) error {
	for {
		cols, err := plan.Next()
		if err != nil {
			return err
		}
		if cols == nil {
			break
		}
	}
	return nil
}

func getRowsBatch(plan FinalPlan) error {
	for {
		rows, err := plan.Batch()
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			break
		}
	}
	return nil
}
