package forget

import "strings"

// EventType indicates the nature of a notification event. It is also used to maske which events should trigger
// a notification.
type EventType int

const (

	// Hit events are sent when a cache item was hit.
	Hit EventType = 1 << iota

	// Miss events are sent when a cache item was missed.
	Miss

	// Set events are sent when a cache item was stored.
	Set

	// Delete events are sent when a cache item was deleted (explicitly calling Delete() or overwritten by Set()).
	Delete

	// WriteComplete events are sent when a cache item's write is finished.
	WriteComplete

	// Expire events are sent when a cache item was detected to be expired. Always together with Delete and Miss.
	Expire

	// Evict events are sent when a cache item was evicted from the cache. Always together with Delete.
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

// Event objects describe an internal change or other event in the cache.
type Event struct {

	// Type indicates the reason of the event.
	Type EventType

	// Keyspace contains the keyspace of the item if an event is related to a single item.
	Keyspace string

	// Key contains the key of the item if an event is related to a single item.
	Key string

	// EffectiveSizeChange contains the net size change caused by the event.
	EffectiveSizeChange int

	// UsedSizeChange contains the size change caused by the event, calculated based on the freed or
	// allocated chunks.
	UsedSizeChange int
}

type notify struct {
	mask     EventType
	listener chan<- *Event
}

// Stats objects contain cache statistics about keyspaces, internal cache segments or the complete cache.
type Stats struct {

	// ItemCount indicates the number of stored items.
	ItemCount int

	// EffectiveSize indicates the net size of the stored items.
	EffectiveSize int

	// UsedSize indicates the total size of the used chunks.
	UsedSize int

	// Readers indicates how many readers were creaetd and not yet finished (closed).
	Readers int

	// MarkedDeleted indicates how many readers were created whose items were deleted since then but the
	// readers are still not done.
	MarkedDeleted int

	// Writers indicates how many writers were created and not yet completed (closed).
	Writers int

	// WritersBlocked indicates how many writers were created whose write is blocked due to active readers
	// preventing eviction.
	WritersBlocked int

	// KeyCollisions indicates the total number of colliding keys in the hash buckets.
	KeyCollisions int
}

// SegmentStats objects contain statistics about an internal cache segment.
type SegmentStats struct {

	// Total contains the statistics about a cache segment.
	Total *Stats

	// AvailableMemory tells how many memory is avalable in a cache segment for new items or further writing.
	AvailableMemory int

	// Keyspaces contain statistics split by keyspaces in a cache segment.
	Keyspaces map[string]*Stats

	chunkSize int
	notify    *notify
}

// CacheStats objects contain statistics about the cache, including the internal cache instnaces and keyspaces.
type CacheStats struct {

	// Total contains statistics the cache.
	Total *Stats

	// AvailableMemory tells how many memory is avalable in the cache for new items or further writing.
	AvailableMemory int

	// Keyspaces contain statistics split by keyspaces in the cache.
	Keyspaces map[string]*Stats

	// Segments contains statistics split by the internal cache segments.
	Segments []*SegmentStats
}

// String returns the string representation of an EventType value, listing all the set flags.
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

// Is checks if one or more EventType flags are set.
func (et EventType) Is(test EventType) bool {
	return et&test != 0
}

func newNotify(listener chan<- *Event, mask EventType) *notify {
	return &notify{
		listener: listener,
		mask:     mask,
	}
}

// forwards an event if it matches the mask
func (n *notify) send(i *Event) {
	if i.Type.Is(n.mask) {
		n.listener <- i
	}
}

// adds every field of the argument stat to the according field in the current stat
func (s *Stats) add(d *Stats) {
	s.ItemCount += d.ItemCount
	s.EffectiveSize += d.EffectiveSize
	s.UsedSize += d.UsedSize
	s.Readers += d.Readers
	s.MarkedDeleted += d.MarkedDeleted
	s.Writers += d.Writers
	s.WritersBlocked += d.WritersBlocked
	s.KeyCollisions += d.KeyCollisions
}

func newSegmentStats(chunkSize, availableMemory int, n *notify) *SegmentStats {
	return &SegmentStats{
		Total:           &Stats{},
		AvailableMemory: availableMemory,
		Keyspaces:       make(map[string]*Stats),
		chunkSize:       chunkSize,
		notify:          n,
	}
}

// calculates how much size a net size value takes in an item based on the chunk size and the current chunk
// offset
func usedSize(size, offset, chunkSize int) int {
	size += offset % chunkSize
	usedSize := (size / chunkSize) * chunkSize
	if size%chunkSize > 0 {
		usedSize += chunkSize
	}

	return usedSize
}

func (s *SegmentStats) notifyHit(keyspace, key string) {
	s.notify.send(&Event{
		Type:     Hit,
		Keyspace: keyspace,
		Key:      key,
	})
}

func (s *SegmentStats) notifyMiss(keyspace, key string) {
	s.notify.send(&Event{
		Type:     Miss,
		Keyspace: keyspace,
		Key:      key,
	})
}

func (s *SegmentStats) notifySet(keyspace, key string, keySize int) {
	s.notify.send(&Event{
		Type:                Set,
		Keyspace:            keyspace,
		Key:                 key,
		EffectiveSizeChange: keySize,
		UsedSizeChange:      usedSize(keySize, 0, s.chunkSize),
	})
}

func (s *SegmentStats) notifyWriteComplete(keyspace string, keySize, contentSize int) {
	s.notify.send(&Event{
		Type:                WriteComplete,
		Keyspace:            keyspace,
		EffectiveSizeChange: contentSize,
		UsedSizeChange:      usedSize(contentSize, keySize, s.chunkSize),
	})
}

func (s *SegmentStats) notifyRemove(typ EventType, keyspace, key string, size int) {
	s.notify.send(&Event{
		Type:                typ,
		Keyspace:            keyspace,
		Key:                 key,
		EffectiveSizeChange: -size,
		UsedSizeChange:      -usedSize(size, 0, s.chunkSize),
	})
}

func (s *SegmentStats) notifyDelete(keyspace, key string, size int) {
	s.notifyRemove(Delete, keyspace, key, size)
}

func (s *SegmentStats) notifyExpire(keyspace, key string) {
	s.notifyRemove(Expire|Miss, keyspace, key, 0)
}

func (s *SegmentStats) notifyExpireDelete(keyspace, key string, size int) {
	s.notifyRemove(Expire|Delete|Miss, keyspace, key, size)
}

func (s *SegmentStats) notifyEvict(keyspace string, size int) {
	s.notifyRemove(Evict|Delete, keyspace, "", size)
}

func (s *SegmentStats) notifyAllocFailed(keyspace string) {
	s.notify.send(&Event{
		Type:     AllocFailed,
		Keyspace: keyspace,
	})
}

// tracks the changes of all the indicators caused by an added or removed item
func (s *SegmentStats) itemChange(i *item, mod int) {
	size := i.size * mod
	usedSize := usedSize(i.size, 0, s.chunkSize) * mod

	s.Total.ItemCount += mod
	s.Total.EffectiveSize += size
	s.Total.UsedSize += usedSize
	s.AvailableMemory -= usedSize

	ks := s.Keyspaces[i.keyspace]
	ks.ItemCount += mod
	ks.EffectiveSize += size
	ks.UsedSize += usedSize
}

func (s *SegmentStats) addItem(i *item) {
	if _, ok := s.Keyspaces[i.keyspace]; !ok {
		s.Keyspaces[i.keyspace] = &Stats{}
	}

	s.itemChange(i, 1)
}

func (s *SegmentStats) deleteItem(i *item) {
	s.itemChange(i, -1)
	if s.Keyspaces[i.keyspace].ItemCount == 0 {
		delete(s.Keyspaces, i.keyspace)
	}
}

func (s *SegmentStats) readersChange(keyspace string, d int) {
	s.Total.Readers += d
	if ks, ok := s.Keyspaces[keyspace]; ok {
		ks.Readers += d
	}
}

func (s *SegmentStats) readersOnDeletedChange(keyspace string, d int) {
	s.Total.MarkedDeleted += d
	if ks, ok := s.Keyspaces[keyspace]; ok {
		ks.MarkedDeleted += d
	}
}

func (s *SegmentStats) writersChange(keyspace string, d int) {
	s.Total.Writers += d
	if ks, ok := s.Keyspaces[keyspace]; ok {
		ks.Writers += d
	}
}

func (s *SegmentStats) blockedWritersChange(keyspace string, d int) {
	s.Total.WritersBlocked += d
	if ks, ok := s.Keyspaces[keyspace]; ok {
		ks.WritersBlocked += d
	}
}

func (s *SegmentStats) keyCollisionsChange(keyspace string, d int) {
	s.Total.KeyCollisions += d
	if ks, ok := s.Keyspaces[keyspace]; ok {
		ks.KeyCollisions += d
	}
}

func (s *SegmentStats) incReaders(keyspace string)        { s.readersChange(keyspace, 1) }
func (s *SegmentStats) decReaders(keyspace string)        { s.readersChange(keyspace, -1) }
func (s *SegmentStats) incMarkedDeleted(keyspace string)  { s.readersOnDeletedChange(keyspace, 1) }
func (s *SegmentStats) decMarkedDeleted(keyspace string)  { s.readersOnDeletedChange(keyspace, -1) }
func (s *SegmentStats) incWriters(keyspace string)        { s.writersChange(keyspace, 1) }
func (s *SegmentStats) decWriters(keyspace string)        { s.writersChange(keyspace, -1) }
func (s *SegmentStats) incWritersBlocked(keyspace string) { s.blockedWritersChange(keyspace, 1) }
func (s *SegmentStats) decWritersBlocked(keyspace string) { s.blockedWritersChange(keyspace, -1) }
func (s *SegmentStats) incKeyCollisions(keyspace string)  { s.keyCollisionsChange(keyspace, 1) }
func (s *SegmentStats) decKeyCollisions(keyspace string)  { s.keyCollisionsChange(keyspace, -1) }

func (s *SegmentStats) clone() *SegmentStats {
	sc := &SegmentStats{
		AvailableMemory: s.AvailableMemory,
		Keyspaces:       make(map[string]*Stats),
	}

	st := *s.Total
	sc.Total = &st

	for k, ks := range s.Keyspaces {
		kss := *ks
		sc.Keyspaces[k] = &kss
	}

	return sc
}

func newCacheStats(i []*SegmentStats) *CacheStats {
	s := &CacheStats{
		Total:     &Stats{},
		Keyspaces: make(map[string]*Stats),
		Segments:  make([]*SegmentStats, 0, len(i)),
	}

	for _, ii := range i {
		s.Total.add(ii.Total)
		s.AvailableMemory += ii.AvailableMemory
		s.Segments = append(s.Segments, ii)

		for k, ks := range ii.Keyspaces {
			if _, ok := s.Keyspaces[k]; !ok {
				s.Keyspaces[k] = &Stats{}
			}

			s.Keyspaces[k].add(ks)
		}
	}

	return s
}
