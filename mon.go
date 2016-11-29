package forget

import "strings"

// EventType indicates the nature of a notification event. It also can be used to masked which events should
// cause a notification.
type EventType int

const (

	// Hit events are sent when a cache item was hit.
	Hit EventType = 1 << iota

	// Miss events are sent when a cache item was missed.
	Miss

	// Set events are sent when a cache item was stored.
	Set

	// Delete events are sent when a cache item was deleted (explicitly calling Del()).
	Delete

	// WriteComplete events are sent when a cache item's write is finished.
	WriteComplete

	// Expire events are sent when a cache item was detected to be expired. Always together with Miss.
	Expire

	// Evict events are sent when a cache item was evicted from the cache.
	Evict

	// AllocFailed events are sent when allocation for a new item or when writing to an item couldn't complete.
	AllocFailed

	// Normal mask for receiving moderate level of notifications.
	Normal = Evict | AllocFailed

	// Verbose mask for receiving verbose level of notifications.
	Verbose = Miss | Normal

	// All mask for receiving all possible notifications.
	All = Hit | Set | Delete | WriteComplete | Expire | Verbose
)

// Event objects describe the cause of a notification.
type Event struct {
	Type                                EventType
	Keyspace, Key                       string
	EffectiveSizeChange, UsedSizeChange int
}

type notify struct {
	mask     EventType
	listener chan<- *Event
}

// Status objects contain cache statistics about keyspaces, internal cache instances or the complete cache.
type Status struct {
	ItemCount        int
	EffectiveSize    int
	UsedSize         int
	Readers          int
	ReadersOnDeleted int
	Writers          int
	WritersBlocked   int
}

// InstanceStatus objects contain statistics about an internal cache instance.
type InstanceStatus struct {
	Total           *Status
	AvailableMemory int
	Keyspaces       map[string]*Status
	segmentSize     int
	notify          *notify
}

// CacheStatus objects contain statistics about the cache, including the internal cache instnaces and keyspaces.
type CacheStatus struct {
	Total           *Status
	AvailableMemory int
	Keyspaces       map[string]*Status
	Instances       []*InstanceStatus
}

func (et EventType) String() string {
	switch et {
	case Hit:
		return "hit"
	case Miss:
		return "miss"
	case Set:
		return "set"
	case Delete:
		return "delete"
	case WriteComplete:
		return "writecomplete"
	case Expire:
		return "expire"
	case Evict:
		return "evict"
	case AllocFailed:
		return "allocFailed"
	default:
		var (
			s []string
			p uint
		)

		et &= All
		for et > 0 {
			if et%2 == 1 {
				s = append(s, EventType(1<<p).String())
			}

			et >>= 1
			p++
		}

		return strings.Join(s, "|")
	}
}

func usedSize(size, offset, segmentSize int) int {
	size += offset % segmentSize
	usedSize := (size / segmentSize) * segmentSize
	if size%segmentSize > 0 {
		usedSize += segmentSize
	}

	return usedSize
}

func newNotify(listener chan<- *Event, mask EventType) *notify {
	return &notify{
		listener: listener,
		mask:     mask,
	}
}

func (n *notify) send(e *Event) {
	if n.mask&e.Type == 0 {
		return
	}

	n.listener <- e
}

func (s *Status) add(d *Status) {
	s.ItemCount += d.ItemCount
	s.EffectiveSize += d.EffectiveSize
	s.UsedSize += d.UsedSize
	s.Readers += d.Readers
	s.ReadersOnDeleted += d.ReadersOnDeleted
	s.Writers += d.Writers
	s.WritersBlocked += d.WritersBlocked
}

func newInstanceStatus(segmentSize, availableMemory int, n *notify) *InstanceStatus {
	return &InstanceStatus{
		Total:           new(Status),
		AvailableMemory: availableMemory,
		Keyspaces:       make(map[string]*Status),
		segmentSize:     segmentSize,
		notify:          n,
	}
}

func (s *InstanceStatus) sendEvent(e *Event) {
	s.notify.send(e)
}

func (s *InstanceStatus) hit(keyspace, key string) {
	s.sendEvent(&Event{
		Type:     Hit,
		Keyspace: keyspace,
		Key:      key,
	})
}

func (s *InstanceStatus) miss(keyspace, key string) {
	s.sendEvent(&Event{
		Type:     Miss,
		Keyspace: keyspace,
		Key:      key,
	})
}

func (s *InstanceStatus) set(keyspace, key string, keySize int) {
	s.sendEvent(&Event{
		Type:                Set,
		Keyspace:            keyspace,
		Key:                 key,
		EffectiveSizeChange: keySize,
		UsedSizeChange:      usedSize(keySize, 0, s.segmentSize),
	})
}

func (s *InstanceStatus) writeComplete(keyspace string, keySize, contentSize int) {
	s.sendEvent(&Event{
		Type:                WriteComplete,
		Keyspace:            keyspace,
		EffectiveSizeChange: contentSize,
		UsedSizeChange:      usedSize(contentSize, keySize, s.segmentSize),
	})
}

func (s *InstanceStatus) remove(typ EventType, keyspace, key string, size int) {
	s.sendEvent(&Event{
		Type:                typ,
		Keyspace:            keyspace,
		Key:                 key,
		EffectiveSizeChange: -size,
		UsedSizeChange:      -usedSize(size, 0, s.segmentSize),
	})
}

func (s *InstanceStatus) del(keyspace, key string, size int) { s.remove(Delete, keyspace, key, size) }
func (s *InstanceStatus) expire(keyspace, key string, size int) {
	s.remove(Expire|Delete|Miss, keyspace, key, size)
}
func (s *InstanceStatus) evict(keyspace string, size int) { s.remove(Evict|Delete, keyspace, "", size) }

func (s *InstanceStatus) allocFailed(keyspace string) {
	s.sendEvent(&Event{
		Type:     AllocFailed,
		Keyspace: keyspace,
	})
}

func (s *InstanceStatus) updateStatus(e *entry, mod int) {
	size := e.size * mod
	usedSize := usedSize(e.size, 0, s.segmentSize) * mod

	s.Total.ItemCount += mod
	s.Total.EffectiveSize += size
	s.Total.UsedSize += usedSize
	s.AvailableMemory -= usedSize

	ks := s.Keyspaces[e.keyspace]
	ks.ItemCount += mod
	ks.EffectiveSize += size
	ks.UsedSize += usedSize
}

func (s *InstanceStatus) itemAdded(e *entry) {
	if _, ok := s.Keyspaces[e.keyspace]; !ok {
		s.Keyspaces[e.keyspace] = new(Status)
	}

	s.updateStatus(e, 1)
}

func (s *InstanceStatus) itemDeleted(e *entry) {
	s.updateStatus(e, -1)
	if s.Keyspaces[e.keyspace].ItemCount == 0 {
		delete(s.Keyspaces, e.keyspace)
	}
}

func (s *InstanceStatus) addReaders(keyspace string, d int) {
	s.Total.Readers += d
	if ks, ok := s.Keyspaces[keyspace]; ok {
		ks.Readers += d
	}
}

func (s *InstanceStatus) addReadersOnDeleted(keyspace string, d int) {
	s.Total.ReadersOnDeleted += d
	if ks, ok := s.Keyspaces[keyspace]; ok {
		ks.ReadersOnDeleted += d
	}
}

func (s *InstanceStatus) addWriters(keyspace string, d int) {
	s.Total.Writers += d
	if ks, ok := s.Keyspaces[keyspace]; ok {
		ks.Writers += d
	}
}

func (s *InstanceStatus) addWritersBlocked(keyspace string, d int) {
	s.Total.WritersBlocked += d
	if ks, ok := s.Keyspaces[keyspace]; ok {
		ks.WritersBlocked += d
	}
}

func (s *InstanceStatus) incReaders(keyspace string)          { s.addReaders(keyspace, 1) }
func (s *InstanceStatus) decReaders(keyspace string)          { s.addReaders(keyspace, -1) }
func (s *InstanceStatus) incReadersOnDeleted(keyspace string) { s.addReadersOnDeleted(keyspace, 1) }
func (s *InstanceStatus) decReadersOnDeleted(keyspace string) { s.addReadersOnDeleted(keyspace, -1) }
func (s *InstanceStatus) incWriters(keyspace string)          { s.addWriters(keyspace, 1) }
func (s *InstanceStatus) decWriters(keyspace string)          { s.addWriters(keyspace, -1) }
func (s *InstanceStatus) incWritersBlocked(keyspace string)   { s.addWritersBlocked(keyspace, 1) }
func (s *InstanceStatus) decWritersBlocked(keyspace string)   { s.addWritersBlocked(keyspace, -1) }

func (s *InstanceStatus) clone() *InstanceStatus {
	sc := &InstanceStatus{
		AvailableMemory: s.AvailableMemory,
		Keyspaces:       make(map[string]*Status),
	}

	st := *s.Total
	sc.Total = &st

	for k, ks := range s.Keyspaces {
		kss := *ks
		sc.Keyspaces[k] = &kss
	}

	return sc
}

func newCacheStatus(i []*InstanceStatus) *CacheStatus {
	s := &CacheStatus{
		Total:     &Status{},
		Keyspaces: make(map[string]*Status),
		Instances: make([]*InstanceStatus, 0, len(i)),
	}

	for _, ii := range i {
		s.Total.add(ii.Total)
		s.AvailableMemory += ii.AvailableMemory
		s.Instances = append(s.Instances, ii.clone())

		for k, ks := range ii.Keyspaces {
			if _, ok := s.Keyspaces[k]; !ok {
				s.Keyspaces[k] = new(Status)
			}

			s.Keyspaces[k].add(ks)
		}
	}

	return s
}
