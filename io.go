package forget

import (
	"sync"
)

type cio struct {
	mx     *sync.Mutex
	entry  *entry
	offset int
}

func newIO(mx *sync.Mutex, e *entry) *cio {
	return &cio{mx: mx, entry: e}
}

func (cio *cio) readOnce(p []byte) (int, error) {
	cio.mx.Lock()
	defer cio.mx.Unlock()
	return cio.entry.read(cio.offset, p)
}

func (cio *cio) Read(p []byte) (int, error) {
	n, err := cio.readOnce(p)
	for n == 0 && err == nil {
		cio.entry.waitData()
		n, err = cio.readOnce(p)
	}

	cio.offset += n
	return n, err
}

func (cio *cio) Write(p []byte) (int, error) {
	n, err := func() (int, error) {
		cio.mx.Lock()
		defer cio.mx.Unlock()

		n, err := cio.entry.write(p)
		cio.offset += n
		return n, err
	}()

	cio.entry.broadcastData()
	return n, err
}

func (cio *cio) Close() error {
	err := func() error {
		cio.mx.Lock()
		defer cio.mx.Unlock()
		return cio.entry.closeWrite()
	}()

	cio.entry.broadcastData()
	return err
}
