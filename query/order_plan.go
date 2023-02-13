package query

import (
	"bytes"
	"container/heap"
	"fmt"
	"strconv"
	"strings"
)

type FinalOrderPlan struct {
	Txn        Txn
	Orders     []OrderField
	FieldNames []string
	ChildPlan  FinalPlan
	pos        int
	total      int
	sorted     *orderColumnsRowHeap
	orderPos   []int
	orderTypes []Type
}

func (p *FinalOrderPlan) findOrderIdx(o OrderField) (int, error) {
	fname := o.Name
	for i, fn := range p.FieldNames {
		if fname == fn {
			return i, nil
		}
	}
	return 0, fmt.Errorf("Cannot find field: %s", fname)
}

func (p *FinalOrderPlan) Init() error {
	p.pos = 0
	p.total = 0
	p.orderPos = []int{}
	p.orderTypes = []Type{}
	for _, o := range p.Orders {
		idx, err := p.findOrderIdx(o)
		if err != nil {
			return err
		}
		p.orderPos = append(p.orderPos, idx)
		p.orderTypes = append(p.orderTypes, o.Field.ReturnType())
	}
	p.sorted = &orderColumnsRowHeap{}
	heap.Init(p.sorted)
	return p.ChildPlan.Init()
}

func (p *FinalOrderPlan) FieldNameList() []string {
	return p.FieldNames
}

func (p *FinalOrderPlan) String() string {
	fields := []string{}
	for _, f := range p.Orders {
		orderStr := " ASC"
		if f.Order == DESC {
			orderStr = " DESC"
		}
		fields = append(fields, f.Name+orderStr)
	}
	return fmt.Sprintf("OrderPlan{Fields = <%s>}", strings.Join(fields, ", "))
}

func (p *FinalOrderPlan) Explain() []string {
	ret := []string{p.String()}
	for _, plan := range p.ChildPlan.Explain() {
		ret = append(ret, plan)
	}
	return ret
}

func (p *FinalOrderPlan) Next() ([]Column, error) {
	if p.total == 0 {
		if err := p.prepare(); err != nil {
			return nil, err
		}
	}
	if p.pos < p.total {
		rrow := heap.Pop(p.sorted)
		row := rrow.(*orderColumnsRow)
		p.pos++
		return row.cols, nil
	}
	return nil, nil
}

func (p *FinalOrderPlan) prepare() error {
	for {
		col, err := p.ChildPlan.Next()
		if err != nil {
			return err
		}
		if col == nil && err == nil {
			break
		}
		row := &orderColumnsRow{
			cols:       col,
			orders:     p.Orders,
			orderPos:   p.orderPos,
			orderTypes: p.orderTypes,
		}
		heap.Push(p.sorted, row)
		p.total++
	}
	return nil
}

type orderColumnsRow struct {
	cols       []Column
	orders     []OrderField
	orderPos   []int
	orderTypes []Type
}

func (l *orderColumnsRow) Less(r *orderColumnsRow) bool {
	for i, o := range l.orders {
		oidx := l.orderPos[i]
		desc := o.Order == DESC
		var compare int
		lval := l.cols[oidx]
		rval := r.cols[oidx]
		switch l.orderTypes[i] {
		case TSTR:
			compare = l.compareBytes(lval, rval, desc)
		case TBOOL:
			compare = l.compareBool(lval, rval, desc)
		case TNUMBER:
			compare = l.compareNumber(lval, rval, desc)
		default:
			return false
		}
		if compare < 0 {
			return true
		} else if compare > 0 {
			return false
		}
	}
	return false
}

func (l *orderColumnsRow) compareBytes(lval, rval []byte, reverse bool) int {
	if reverse {
		return 0 - bytes.Compare(lval, rval)
	}
	return bytes.Compare(lval, rval)
}

func (l *orderColumnsRow) compareBool(lval, rval []byte, reverse bool) int {
	lbool := bytes.Equal(lval, []byte("true"))
	rbool := bytes.Equal(rval, []byte("true"))
	lint := 0
	rint := 0
	if lbool {
		lint = 1
	}
	if rbool {
		rint = 1
	}
	if lint == rint {
		return 0
	}
	if reverse {
		if lint > rint {
			return -1
		} else {
			return 1
		}
	}
	if lint < rint {
		return -1
	} else {
		return 1
	}
}

func (l *orderColumnsRow) compareNumber(lval, rval []byte, reverse bool) int {
	var (
		lint, rint     int64
		lfloat, rfloat float64
		err            error
	)
	if lint, err = strconv.ParseInt(string(lval), 10, 64); err == nil {
		if rint, err = strconv.ParseInt(string(rval), 10, 64); err == nil {
			return l.compareInt(lint, rint, reverse)
		}
	}

	if lfloat, err = strconv.ParseFloat(string(lval), 64); err == nil {
		if rfloat, err = strconv.ParseFloat(string(rval), 64); err == nil {
			return l.compareFloat(lfloat, rfloat, reverse)
		}
	}
	return 0
}

func (l *orderColumnsRow) compareInt(lval, rval int64, reverse bool) int {
	if lval == rval {
		return 0
	}
	if reverse {
		if lval > rval {
			return -1
		} else {
			return 1
		}
	}
	if lval < rval {
		return -1
	} else {
		return 1
	}
}

func (l *orderColumnsRow) compareFloat(lval, rval float64, reverse bool) int {
	if lval == rval {
		return 0
	}
	if reverse {
		if lval > rval {
			return -1
		} else {
			return 1
		}
	}
	if lval < rval {
		return -1
	} else {
		return 1
	}
}

type orderColumnsRowHeap []*orderColumnsRow

func (h orderColumnsRowHeap) Len() int {
	return len(h)
}

func (h orderColumnsRowHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *orderColumnsRowHeap) Push(x any) {
	*h = append(*h, x.(*orderColumnsRow))
}

func (h *orderColumnsRowHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h orderColumnsRowHeap) Less(i, j int) bool {
	l := h[i]
	r := h[j]
	return l.Less(r)
}
