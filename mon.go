package forget

type Status struct {
	ItemCount        int
	EffectiveSize    int
	UsedSize         int
	Readers          int
	ReadersOnDeleted int
	Writers          int
	WritersBlocked   int
	Keyspaces        int
}

type InstanceStatus struct {
	Total           *Status
	AvailableMemory int
	Keyspaces       map[string]*Status
}

type CacheStatus struct {
	Total     *Status
	Instances []*InstanceStatus
}

type EventType int

const (
	Hit EventType = 1 << iota
	Miss
	Set
	Delete
	Expire
	Evict
	AllocFailed

	None    = 0
	Normal  = Evict | AllocFailed
	Verbose = Miss | Normal
	All     = Hit | Set | Delete | Expire | Verbose
)

type Event struct {
	Type                                EventType
	Keyspace, Key                       string
	EffectiveSizeChange, UsedSizeChange int
	Status                              *CacheStatus
	status                              *InstanceStatus
	cpuIndex                            int
}

type notify struct {
	mask     EventType
	listener chan<- *Event
}

type demuxNotify struct {
	listener chan<- *Event
}

// [status]
//
// readers
// blocked from delete
// number of items
// number of items per keyspace
// item size
// item size per keyspace
// free memory
// effective size
// used size
// per cpu
// active writers
// blocked writers

// [event]
//
// hit/miss
// eviction
// expired
// set/delete
// allocation failed
