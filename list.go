package forget

type node interface {
	prev() node
	next() node
	setPrev(node)
	setNext(node)
}

type list struct {
	first, last        node
	prevList, nextList node
}

// implements list of lists:
func (l *list) prev() node     { return l.prevList }
func (l *list) next() node     { return l.nextList }
func (l *list) setPrev(p node) { l.prevList = p }
func (l *list) setNext(n node) { l.nextList = n }

// inserts a range before a node in the list. If the before argument is nil, it inserts the range at the end of
// the list
func (l *list) insertRange(first, last, before node) {
	var prev node
	if before == nil {
		prev = l.last
	} else {
		prev = before.prev()
	}

	next := before
	first.setPrev(prev)
	last.setNext(next)

	if prev == nil {
		l.first = first
		if first != nil {
			first.setPrev(nil)
		}
	} else {
		prev.setNext(first)
	}

	if next == nil {
		l.last = last
		if last != nil {
			last.setNext(nil)
		}
	} else {
		next.setPrev(last)
	}
}

// removes a range from the list
func (l *list) removeRange(first, last node) {
	prev, next := first.prev(), last.next()

	if prev == nil {
		l.first = next
		if next != nil {
			next.setPrev(nil)
		}
	} else {
		prev.setNext(next)
	}

	if next == nil {
		l.last = prev
		if prev != nil {
			prev.setNext(nil)
		}
	} else {
		next.setPrev(prev)
	}
}

// removes the range from the list that is before the argument node, and inserts it at the end of the remaining
// list
func (l *list) rotate(at node) {
	if at == nil || l.empty() {
		return
	}

	from := l.first
	l.removeRange(from, at)
	l.insertRange(from, at, nil)
}

func (l *list) insert(n, before node) { l.insertRange(n, n, before) }
func (l *list) remove(n node)         { l.removeRange(n, n) }
func (l *list) empty() bool           { return l.first == nil }
