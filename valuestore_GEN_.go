package replstore

import (
	"fmt"

	"github.com/gholt/store"
)

type replValueStore struct {
	stores []store.ValueStore
}

type replValueStoreErrors struct {
	errs []*replValueStoreError
}

func (e *replValueStoreErrors) Error() string {
	if len(e.errs) <= 0 {
		return "unknown error"
	} else if len(e.errs) == 1 {
		return e.errs[0].Error()
	}
	return fmt.Sprintf("%d errors, first is: %s", len(e.errs), e.errs[0])
}

type replValueStoreError struct {
	store store.ValueStore
	err   error
}

func (e *replValueStoreError) Error() string {
	if e.err == nil {
		return "unknown error"
	}
	return e.err.Error()
}

func NewReplValueStore(stores []store.ValueStore) store.ValueStore {
	return &replValueStore{stores: stores}
}

func (rs *replValueStore) helper(f func(s store.ValueStore) error) error {
	ec := make(chan *replValueStoreError)
	for _, s := range rs.stores {
		go func(s store.ValueStore) {
			if err := f(s); err != nil {
				ec <- &replValueStoreError{store: s, err: err}
			} else {
				ec <- nil
			}
		}(s)
	}
	var errs []*replValueStoreError
	for _ = range rs.stores {
		if err := <-ec; err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return &replValueStoreErrors{errs: errs}
	}
	return nil
}

func (rs *replValueStore) Startup() error {
	return rs.helper(func(s store.ValueStore) error { return s.Startup() })
}

func (rs *replValueStore) Shutdown() error {
	return rs.helper(func(s store.ValueStore) error { return s.Shutdown() })
}

func (rs *replValueStore) EnableWrites() error {
	return rs.helper(func(s store.ValueStore) error { return s.EnableWrites() })
}

func (rs *replValueStore) DisableWrites() error {
	return rs.helper(func(s store.ValueStore) error { return s.DisableWrites() })
}

func (rs *replValueStore) Flush() error {
	return rs.helper(func(s store.ValueStore) error { return s.Flush() })
}

func (rs *replValueStore) AuditPass() error {
	return rs.helper(func(s store.ValueStore) error { return s.AuditPass() })
}

func (rs *replValueStore) Stats(debug bool) (fmt.Stringer, error) {
	return nil, nil
}

func (rs *replValueStore) ValueCap() (uint32, error) {
	return 0, nil
}

func (rs *replValueStore) Lookup(keyA, keyB uint64) (int64, uint32, error) {
	return 0, 0, nil
}

func (rs *replValueStore) Read(keyA uint64, keyB uint64, value []byte) (int64, []byte, error) {
	return 0, nil, nil
}

func (rs *replValueStore) Write(keyA uint64, keyB uint64, timestampmicro int64, value []byte) (int64, error) {
	return 0, nil
}

func (rs *replValueStore) Delete(keyA uint64, keyB uint64, timestampmicro int64) (int64, error) {
	return 0, nil
}
