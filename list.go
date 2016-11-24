package forget

type node interface {
	prev() node
	next() node
	setPrev(node)
	setNext(node)
}

type list struct {
	first, last node
}

func (l *list) appendRange(first, last node) {
	first.setPrev(l.last)
	last.setNext(nil)

	if l.first == nil {
		l.first = first
	} else {
		l.last.setNext(first)
	}

	l.last = last
}

func (l *list) insertRange(first, last, before node) {
	if before == nil {
		l.appendRange(first, last)
		return
	}

	prev, next := before.prev(), before
	first.setPrev(prev)
	last.setNext(next)

	if prev == nil {
		l.first = first
	} else {
		prev.setNext(first)
	}

	next.setPrev(last)
}

func (l *list) removeRange(first, last node) {
	prev, next := first.prev(), last.next()

	if prev == nil {
		l.first = next
	} else {
		prev.setNext(next)
	}

	if next == nil {
		l.last = prev
	} else {
		next.setPrev(prev)
	}
}

func (l *list) append(n node)         { l.appendRange(n, n) }
func (l *list) insert(n, before node) { l.insertRange(n, n, before) }
func (l *list) remove(n node)         { l.removeRange(n, n) }
