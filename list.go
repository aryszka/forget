package forget

type node interface {
	prev() node
	next() node
	setPrev(node)
	setNext(node)
}

type list struct {
	first, last        node
	prevNode, nextNode node
}

func (l *list) prev() node     { return l.prevNode }
func (l *list) next() node     { return l.nextNode }
func (l *list) setPrev(p node) { l.prevNode = p }
func (l *list) setNext(n node) { l.nextNode = n }

func (l *list) firstLast() {
	if l.first != nil {
		l.first.setPrev(nil)
		l.last.setNext(nil)
	}
}

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
	} else {
		prev.setNext(first)
	}

	if next == nil {
		l.last = last
	} else {
		next.setPrev(last)
	}

	l.firstLast()
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

	l.firstLast()
}

func (l *list) insert(n, before node) { l.insertRange(n, n, before) }
func (l *list) remove(n node)         { l.removeRange(n, n) }
func (l *list) empty() bool           { return l.first == nil }
