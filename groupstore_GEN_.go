package replstore

import (
	"fmt"

	"github.com/gholt/store"
)

type replGroupStore struct {
	stores []store.GroupStore
}

type replGroupStoreErrors struct {
	errs []*replGroupStoreError
}

func (e *replGroupStoreErrors) Error() string {
	if len(e.errs) <= 0 {
		return "unknown error"
	} else if len(e.errs) == 1 {
		return e.errs[0].Error()
	}
	return fmt.Sprintf("%d errors, first is: %s", len(e.errs), e.errs[0])
}

type replGroupStoreError struct {
	store store.GroupStore
	err   error
}

func (e *replGroupStoreError) Error() string {
	if e.err == nil {
		return "unknown error"
	}
	return e.err.Error()
}

func NewReplGroupStore(stores []store.GroupStore) store.GroupStore {
	return &replGroupStore{stores: stores}
}

func (rs *replGroupStore) helper(f func(s store.GroupStore) error) error {
	ec := make(chan *replGroupStoreError)
	for _, s := range rs.stores {
		go func(s store.GroupStore) {
			if err := f(s); err != nil {
				ec <- &replGroupStoreError{store: s, err: err}
			} else {
				ec <- nil
			}
		}(s)
	}
	var errs []*replGroupStoreError
	for _ = range rs.stores {
		if err := <-ec; err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return &replGroupStoreErrors{errs: errs}
	}
	return nil
}

func (rs *replGroupStore) Startup() error {
	return rs.helper(func(s store.GroupStore) error { return s.Startup() })
}

func (rs *replGroupStore) Shutdown() error {
	return rs.helper(func(s store.GroupStore) error { return s.Shutdown() })
}

func (rs *replGroupStore) EnableWrites() error {
	return rs.helper(func(s store.GroupStore) error { return s.EnableWrites() })
}

func (rs *replGroupStore) DisableWrites() error {
	return rs.helper(func(s store.GroupStore) error { return s.DisableWrites() })
}

func (rs *replGroupStore) Flush() error {
	return rs.helper(func(s store.GroupStore) error { return s.Flush() })
}

func (rs *replGroupStore) AuditPass() error {
	return rs.helper(func(s store.GroupStore) error { return s.AuditPass() })
}

func (rs *replGroupStore) Stats(debug bool) (fmt.Stringer, error) {
	return nil, nil
}

func (rs *replGroupStore) ValueCap() (uint32, error) {
	return 0, nil
}

func (rs *replGroupStore) Lookup(keyA, keyB uint64, childKeyA, childKeyB uint64) (int64, uint32, error) {
	return 0, 0, nil
}

func (rs *replGroupStore) Read(keyA uint64, keyB uint64, childKeyA, childKeyB uint64, value []byte) (int64, []byte, error) {
	return 0, nil, nil
}

func (rs *replGroupStore) Write(keyA uint64, keyB uint64, childKeyA, childKeyB uint64, timestampmicro int64, value []byte) (int64, error) {
	return 0, nil
}

func (rs *replGroupStore) Delete(keyA uint64, keyB uint64, childKeyA, childKeyB uint64, timestampmicro int64) (int64, error) {
	return 0, nil
}

func (rs *replGroupStore) LookupGroup(parentKeyA, parentKeyB uint64) ([]store.LookupGroupItem, error) {
	return nil, nil
}
