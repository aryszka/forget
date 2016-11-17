package forget

type Status struct {
	Len, Size int
}

type CacheStatus struct {
	Keyspaces map[string]*Status
	Len, Size int
}

func getStatus(space *keyspace) *Status {
	s := &Status{}
	if space == nil {
		return s
	}

	s.Len = len(space.lookup)
	for _, e := range space.lookup {
		s.Size += e.size()
	}

	return s
}

func getCacheStatus(spaces map[string]*keyspace) *CacheStatus {
	cs := &CacheStatus{Keyspaces: make(map[string]*Status)}
	for k, space := range spaces {
		s := getStatus(space)
		cs.Keyspaces[k] = s
		cs.Len += s.Len
		cs.Size += s.Size
	}

	return cs
}

func requestStatus(c *cache, req message) message {
	var rsp message
	switch req.typ {
	case statusMsg:
		rsp.status = getStatus(c.spaces[req.keyspace])
	case cacheStatusMsg:
		rsp.cacheStatus = getCacheStatus(c.spaces)
	}

	return rsp
}
