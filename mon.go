package forget

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
}

// CacheStatus objects contain statistics about the cache, including the internal cache instnaces and keyspaces.
type CacheStatus struct {
	Total           *Status
	AvailableMemory int
	Keyspaces       map[string]*Status
	Instances       []*InstanceStatus
}

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

	// Expire events are sent when a cache item was detected to be expired.
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
	All = Hit | Set | Delete | Expire | Verbose
)

// Event objects describe the cause of a notification.
type Event struct {
	Type                                EventType
	Keyspace, Key                       string
	EffectiveSizeChange, UsedSizeChange int
	Status                              *CacheStatus
	instanceStatus                      *InstanceStatus
	instanceIndex                       int
}

type notify struct {
	mask     EventType
	listener chan<- *Event
}

type demuxNotify struct {
	listener chan<- *Event
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

func newInstanceStatus(segmentSize, availableMemory int) *InstanceStatus {
	return &InstanceStatus{
		Total:           new(Status),
		AvailableMemory: availableMemory,
		Keyspaces:       make(map[string]*Status),
		segmentSize:     segmentSize,
	}
}

func (s *InstanceStatus) updateStatus(e *entry, mod int) {
	usedSize := (e.size / s.segmentSize) * s.segmentSize
	if e.size%s.segmentSize > 0 {
		usedSize += s.segmentSize
	}

	size := e.size * mod
	usedSize *= mod

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
