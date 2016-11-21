package forget

import "testing"

type testNode struct {
	value              int
	prevNode, nextNode node
}

func (n *testNode) prev() node      { return n.prevNode }
func (n *testNode) next() node      { return n.nextNode }
func (n *testNode) setPrev(nn node) { n.prevNode = nn }
func (n *testNode) setNext(nn node) { n.nextNode = nn }

func initList(v ...int) *list {
	l := &list{}
	for _, vi := range v {
		l.append(&testNode{value: vi})
	}

	return l
}

func checkList(t *testing.T, l *list, values ...int) {
	counter := 0
	n, p := l.first, l.last
	var head, tail []int
	for n != nil && p != nil {
		nv, pv := n.(*testNode).value, p.(*testNode).value
		head = append(head, nv)
		tail = append([]int{pv}, tail...)
		if nv != values[counter] || pv != values[len(values)-1-counter] {
			t.Error("invalid list order", head, tail, values)
			return
		}

		counter++
		n, p = n.next(), p.prev()
	}

	if counter != len(values) {
		t.Error("invalid list length", head, tail, values)
	}

	if n != nil || p != nil {
		t.Error("list broken", head, tail, values)
	}
}

func TestListAppend(t *testing.T) {
	t.Run("append to empty list", func(t *testing.T) {
		l := initList()
		l.append(&testNode{value: 1})
		checkList(t, l, 1)
	})

	t.Run("append to list with one item", func(t *testing.T) {
		l := initList(1)
		l.append(&testNode{value: 2})
		checkList(t, l, 1, 2)
	})

	t.Run("append to non-empty list", func(t *testing.T) {
		l := initList(1, 2, 3)
		l.append(&testNode{value: 4})
		checkList(t, l, 1, 2, 3, 4)
	})
}

func TestListInsert(t *testing.T) {
	t.Run("insert into empty list", func(t *testing.T) {
		l := initList()
		l.insert(&testNode{value: 1}, nil)
		checkList(t, l, 1)
	})

	t.Run("insert into list with one item, first", func(t *testing.T) {
		l := initList(1)
		l.insert(&testNode{value: 2}, l.first)
		checkList(t, l, 2, 1)
	})

	t.Run("insert into list with one item, last", func(t *testing.T) {
		l := initList(1)
		l.insert(&testNode{value: 2}, nil)
		checkList(t, l, 1, 2)
	})

	t.Run("insert into non-empty list, first", func(t *testing.T) {
		l := initList(1, 2, 3)
		l.insert(&testNode{value: 4}, l.first)
		checkList(t, l, 4, 1, 2, 3)
	})

	t.Run("insert into non-empty list, between", func(t *testing.T) {
		l := initList(1, 2, 3)
		n := l.first.next().next()
		l.insert(&testNode{value: 4}, n)
		checkList(t, l, 1, 2, 4, 3)
	})

	t.Run("insert into non-empty list, last", func(t *testing.T) {
		l := initList(1, 2, 3)
		l.insert(&testNode{value: 4}, nil)
		checkList(t, l, 1, 2, 3, 4)
	})
}

func TestListRemove(t *testing.T) {
	t.Run("remove from a list with a single item", func(t *testing.T) {
		l := initList(1)
		l.remove(l.first)
		checkList(t, l)
	})

	t.Run("remove first item from a list with two items", func(t *testing.T) {
		l := initList(1, 2)
		l.remove(l.first)
		checkList(t, l, 2)
	})

	t.Run("remove last item from a list with two items", func(t *testing.T) {
		l := initList(1, 2)
		l.remove(l.last)
		checkList(t, l, 1)
	})

	t.Run("remove first item from a list", func(t *testing.T) {
		l := initList(1, 2, 3, 4)
		l.remove(l.first)
		checkList(t, l, 2, 3, 4)
	})

	t.Run("remove last item from a list", func(t *testing.T) {
		l := initList(1, 2, 3, 4)
		l.remove(l.last)
		checkList(t, l, 1, 2, 3)
	})

	t.Run("remove item from a list", func(t *testing.T) {
		l := initList(1, 2, 3, 4)
		l.remove(l.first.next())
		checkList(t, l, 1, 3, 4)
	})
}

func TestListAppendRange(t *testing.T) {
	t.Run("single item to an empty list", func(t *testing.T) {
		l := initList()
		append := initList(1)
		l.appendRange(append.first, append.last)
		checkList(t, l, 1)
	})

	t.Run("multiple items to an empty list", func(t *testing.T) {
		l := initList()
		append := initList(1, 2, 3)
		l.appendRange(append.first, append.last)
		checkList(t, l, 1, 2, 3)
	})

	t.Run("single item to a list with a single item", func(t *testing.T) {
		l := initList(1)
		append := initList(2)
		l.appendRange(append.first, append.last)
		checkList(t, l, 1, 2)
	})

	t.Run("multiple items to a list with a single item", func(t *testing.T) {
		l := initList(1)
		append := initList(2, 3, 4)
		l.appendRange(append.first, append.last)
		checkList(t, l, 1, 2, 3, 4)
	})

	t.Run("single item to a list with multiple items", func(t *testing.T) {
		l := initList(1, 2, 3)
		append := initList(4)
		l.appendRange(append.first, append.last)
		checkList(t, l, 1, 2, 3, 4)
	})

	t.Run("multiple items to a list with multiple items", func(t *testing.T) {
		l := initList(1, 2, 3)
		append := initList(4, 5, 6)
		l.appendRange(append.first, append.last)
		checkList(t, l, 1, 2, 3, 4, 5, 6)
	})
}

func TestListInsertRange(t *testing.T) {
	t.Run("single item into an empty list", func(t *testing.T) {
		l := initList()
		insert := initList(1)
		l.insertRange(insert.first, insert.last, nil)
		checkList(t, l, 1)
	})

	t.Run("multiple items into an empty list", func(t *testing.T) {
		l := initList()
		insert := initList(1, 2, 3)
		l.insertRange(insert.first, insert.last, nil)
		checkList(t, l, 1, 2, 3)
	})

	t.Run("single item into a list with a single item, before", func(t *testing.T) {
		l := initList(1)
		insert := initList(2)
		l.insertRange(insert.first, insert.last, l.first)
		checkList(t, l, 2, 1)
	})

	t.Run("single item into a list with a single item, after", func(t *testing.T) {
		l := initList(1)
		insert := initList(2)
		l.insertRange(insert.first, insert.last, nil)
		checkList(t, l, 1, 2)
	})

	t.Run("multiple items into a list with a single item, before", func(t *testing.T) {
		l := initList(1)
		insert := initList(2, 3, 4)
		l.insertRange(insert.first, insert.last, l.first)
		checkList(t, l, 2, 3, 4, 1)
	})

	t.Run("multiple items into a list with a single item, after", func(t *testing.T) {
		l := initList(1)
		insert := initList(2, 3, 4)
		l.insertRange(insert.first, insert.last, nil)
		checkList(t, l, 1, 2, 3, 4)
	})

	t.Run("single item into a list with multiple items, before", func(t *testing.T) {
		l := initList(1, 2, 3)
		insert := initList(4)
		l.insertRange(insert.first, insert.last, l.first)
		checkList(t, l, 4, 1, 2, 3)
	})

	t.Run("single item into a list with multiple items, after", func(t *testing.T) {
		l := initList(1, 2, 3)
		insert := initList(4)
		l.insertRange(insert.first, insert.last, nil)
		checkList(t, l, 1, 2, 3, 4)
	})

	t.Run("single item into a list with multiple items, between", func(t *testing.T) {
		l := initList(1, 2, 3)
		insert := initList(4)
		l.insertRange(insert.first, insert.last, l.first.next().next())
		checkList(t, l, 1, 2, 4, 3)
	})

	t.Run("multiple items into a list with multiple items, before", func(t *testing.T) {
		l := initList(1, 2, 3)
		insert := initList(4, 5, 6)
		l.insertRange(insert.first, insert.last, l.first)
		checkList(t, l, 4, 5, 6, 1, 2, 3)
	})

	t.Run("multiple items into a list with multiple items, after", func(t *testing.T) {
		l := initList(1, 2, 3)
		insert := initList(4, 5, 6)
		l.insertRange(insert.first, insert.last, nil)
		checkList(t, l, 1, 2, 3, 4, 5, 6)
	})

	t.Run("multiple items into a list with multiple items, between", func(t *testing.T) {
		l := initList(1, 2, 3)
		insert := initList(4, 5, 6)
		l.insertRange(insert.first, insert.last, l.first.next().next())
		checkList(t, l, 1, 2, 4, 5, 6, 3)
	})
}

func TestListRemoveRange(t *testing.T) {
	t.Run("single item from a list with a single item", func(t *testing.T) {
		l := initList(1)
		l.removeRange(l.first, l.first)
		checkList(t, l)
	})

	t.Run("single item from a list with two items, first", func(t *testing.T) {
		l := initList(1, 2)
		l.removeRange(l.first, l.first)
		checkList(t, l, 2)
	})

	t.Run("single item from a list with two items, last", func(t *testing.T) {
		l := initList(1, 2)
		l.removeRange(l.last, l.last)
		checkList(t, l, 1)
	})

	t.Run("both items from a list with two items", func(t *testing.T) {
		l := initList(1, 2)
		l.removeRange(l.first, l.last)
		checkList(t, l)
	})

	t.Run("multiple items from a list, head", func(t *testing.T) {
		l := initList(1, 2, 3, 4)
		l.removeRange(l.first, l.first.next())
		checkList(t, l, 3, 4)
	})

	t.Run("multiple items from a list, tail", func(t *testing.T) {
		l := initList(1, 2, 3, 4)
		l.removeRange(l.last.prev(), l.last)
		checkList(t, l, 1, 2)
	})

	t.Run("multiple items from a list, from between", func(t *testing.T) {
		l := initList(1, 2, 3, 4)
		l.removeRange(l.first.next(), l.last.prev())
		checkList(t, l, 1, 4)
	})

	t.Run("all items from a list", func(t *testing.T) {
		l := initList(1, 2, 3, 4)
		l.removeRange(l.first, l.last)
		checkList(t, l)
	})
}
