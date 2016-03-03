package replstore

import (
	"errors"
	"fmt"
	"sync"

	"github.com/gholt/ring"
	"github.com/gholt/store"
	"github.com/pandemicsyn/oort/api"
)

type ReplGroupStore struct {
	addressIndex int

	ringLock sync.RWMutex
	ring     ring.Ring

	storesLock sync.RWMutex
	stores     map[string]store.GroupStore
}

func NewReplGroupStore(c *ReplGroupStoreConfig) *ReplGroupStore {
	cfg := resolveReplGroupStoreConfig(c)
	return &ReplGroupStore{addressIndex: cfg.AddressIndex}
}

func (rs *ReplGroupStore) Ring() ring.Ring {
	rs.ringLock.RLock()
	r := rs.ring
	rs.ringLock.RUnlock()
	return r
}

func (rs *ReplGroupStore) SetRing(r ring.Ring) {
	rs.ringLock.Lock()
	rs.ring = r
	rs.ringLock.Unlock()
}

func (rs *ReplGroupStore) storesFor(keyA uint64) []store.GroupStore {
	rs.ringLock.RLock()
	r := rs.ring
	rs.ringLock.RUnlock()
	ns := r.ResponsibleNodes(uint32(keyA >> (64 - r.PartitionBitCount())))
	as := make([]string, len(ns))
	for i, n := range ns {
		as[i] = n.Address(rs.addressIndex)
	}
	ss := make([]store.GroupStore, len(ns))
	var someNil bool
	rs.storesLock.RLock()
	for i := len(ss) - 1; i >= 0; i-- {
		ss[i] = rs.stores[as[i]]
		if ss[i] == nil {
			someNil = true
		}
	}
	rs.storesLock.RUnlock()
	if someNil {
		rs.storesLock.Lock()
		for i := len(ss) - 1; i >= 0; i-- {
			if ss[i] == nil {
				ss[i] = rs.stores[as[i]]
				if ss[i] == nil {
					var err error
					ss[i], err = api.NewGroupStore(as[i], 10)
					if err != nil {
						ss[i] = nil
						// TODO: debug log this error
					} else {
						rs.stores[as[i]] = ss[i]
					}
				}
			}
		}
		rs.storesLock.Unlock()
	}
	return ss
}

func (rs *ReplGroupStore) Startup() error {
	return nil
}

func (rs *ReplGroupStore) Shutdown() error {
	return nil
}

func (rs *ReplGroupStore) EnableWrites() error {
	return nil
}

func (rs *ReplGroupStore) DisableWrites() error {
	return errors.New("cannot disable writes with this client at this time")
}

func (rs *ReplGroupStore) Flush() error {
	return nil
}

func (rs *ReplGroupStore) AuditPass() error {
	return errors.New("audit passes not available with this client at this time")
}

func (rs *ReplGroupStore) Stats(debug bool) (fmt.Stringer, error) {
	return noStats, nil
}

func (rs *ReplGroupStore) ValueCap() (uint32, error) {
	return 0xffffffff, nil
}

func (rs *ReplGroupStore) Lookup(keyA, keyB uint64, childKeyA, childKeyB uint64) (int64, uint32, error) {
	type rettype struct {
		timestampMicro int64
		length         uint32
		err            ReplGroupStoreError
	}
	ec := make(chan *rettype)
	stores := rs.storesFor(keyA)
	for _, s := range stores {
		if s == nil {
			continue
		}
		go func(s store.GroupStore) {
			timestampMicro, length, err := s.Lookup(keyA, keyB, childKeyA, childKeyB)
			ret := &rettype{timestampMicro: timestampMicro, length: length}
			if err != nil {
				ret.err = &replGroupStoreError{store: s, err: err}
			}
			ec <- ret
		}(s)
	}
	var timestampMicro int64
	var length uint32
	var errs ReplGroupStoreErrorSlice
	var notFounds int
	// TODO: Selection algorithms
	for _, s := range stores {
		if s == nil {
			errs = append(errs, nilGroupStoreErr)
			continue
		}
		ret := <-ec
		if ret.err != nil {
			errs = append(errs, ret.err)
			if store.IsNotFound(ret.err) {
				notFounds++
			}
		} else if ret.timestampMicro > timestampMicro {
			timestampMicro = ret.timestampMicro
			length = ret.length
		}
	}
	if notFounds > 0 {
		nferrs := make(ReplGroupStoreErrorNotFound, len(errs))
		for i, v := range errs {
			nferrs[i] = v
		}
		return timestampMicro, length, nferrs
	}
	return timestampMicro, length, errs
}

func (rs *ReplGroupStore) Read(keyA uint64, keyB uint64, childKeyA, childKeyB uint64, value []byte) (int64, []byte, error) {
	type rettype struct {
		timestampMicro int64
		value          []byte
		err            ReplGroupStoreError
	}
	ec := make(chan *rettype)
	stores := rs.storesFor(keyA)
	for _, s := range stores {
		if s == nil {
			continue
		}
		go func(s store.GroupStore) {
			timestampMicro, value, err := s.Read(keyA, keyB, childKeyA, childKeyB, nil)
			ret := &rettype{timestampMicro: timestampMicro, value: value}
			if err != nil {
				ret.err = &replGroupStoreError{store: s, err: err}
			}
			ec <- ret
		}(s)
	}
	var timestampMicro int64
	var rvalue []byte
	var errs ReplGroupStoreErrorSlice
	var notFounds int
	// TODO: Selection algorithms
	for _, s := range stores {
		if s == nil {
			errs = append(errs, nilGroupStoreErr)
			continue
		}
		ret := <-ec
		if ret.err != nil {
			errs = append(errs, ret.err)
			if store.IsNotFound(ret.err) {
				notFounds++
			}
		} else if ret.timestampMicro > timestampMicro {
			timestampMicro = ret.timestampMicro
			rvalue = ret.value
		}
	}
	if notFounds > 0 {
		nferrs := make(ReplGroupStoreErrorNotFound, len(errs))
		for i, v := range errs {
			nferrs[i] = v
		}
		return timestampMicro, append(value, rvalue...), nferrs
	}
	return timestampMicro, append(value, rvalue...), errs
}

func (rs *ReplGroupStore) Write(keyA uint64, keyB uint64, childKeyA, childKeyB uint64, timestampMicro int64, value []byte) (int64, error) {
	type rettype struct {
		oldTimestampMicro int64
		err               ReplGroupStoreError
	}
	ec := make(chan *rettype)
	stores := rs.storesFor(keyA)
	for _, s := range stores {
		if s == nil {
			continue
		}
		go func(s store.GroupStore) {
			oldTimestampMicro, err := s.Write(keyA, keyB, childKeyA, childKeyB, timestampMicro, value)
			ret := &rettype{oldTimestampMicro: oldTimestampMicro}
			if err != nil {
				ret.err = &replGroupStoreError{store: s, err: err}
			}
			ec <- ret
		}(s)
	}
	var oldTimestampMicro int64
	var errs ReplGroupStoreErrorSlice
	// TODO: Selection algorithms
	for _, s := range stores {
		if s == nil {
			errs = append(errs, nilGroupStoreErr)
			continue
		}
		ret := <-ec
		if ret.err != nil {
			errs = append(errs, ret.err)
		} else if ret.oldTimestampMicro > oldTimestampMicro {
			oldTimestampMicro = ret.oldTimestampMicro
		}
	}
	return oldTimestampMicro, errs
}

func (rs *ReplGroupStore) Delete(keyA uint64, keyB uint64, childKeyA, childKeyB uint64, timestampMicro int64) (int64, error) {
	type rettype struct {
		oldTimestampMicro int64
		err               ReplGroupStoreError
	}
	ec := make(chan *rettype)
	stores := rs.storesFor(keyA)
	for _, s := range stores {
		if s == nil {
			continue
		}
		go func(s store.GroupStore) {
			oldTimestampMicro, err := s.Delete(keyA, keyB, childKeyA, childKeyB, timestampMicro)
			ret := &rettype{oldTimestampMicro: oldTimestampMicro}
			if err != nil {
				ret.err = &replGroupStoreError{store: s, err: err}
			}
			ec <- ret
		}(s)
	}
	var oldTimestampMicro int64
	var errs ReplGroupStoreErrorSlice
	// TODO: Selection algorithms
	for _, s := range stores {
		if s == nil {
			errs = append(errs, nilGroupStoreErr)
			continue
		}
		ret := <-ec
		if ret.err != nil {
			errs = append(errs, ret.err)
		} else if ret.oldTimestampMicro > oldTimestampMicro {
			oldTimestampMicro = ret.oldTimestampMicro
		}
	}
	return oldTimestampMicro, errs
}

func (rs *ReplGroupStore) LookupGroup(parentKeyA, parentKeyB uint64) ([]store.LookupGroupItem, error) {
	type rettype struct {
		items []store.LookupGroupItem
		err   ReplGroupStoreError
	}
	ec := make(chan *rettype)
	stores := rs.storesFor(parentKeyA)
	for _, s := range stores {
		if s == nil {
			continue
		}
		go func(s store.GroupStore) {
			items, err := s.LookupGroup(parentKeyA, parentKeyB)
			ret := &rettype{items: items}
			if err != nil {
				ret.err = &replGroupStoreError{store: s, err: err}
			}
			ec <- ret
		}(s)
	}
	var items []store.LookupGroupItem
	var errs ReplGroupStoreErrorSlice
	// TODO: Selection algorithms
	for _, s := range stores {
		if s == nil {
			errs = append(errs, nilGroupStoreErr)
			continue
		}
		ret := <-ec
		if ret.err != nil {
			errs = append(errs, ret.err)
		} else if len(ret.items) > len(items) {
			items = ret.items
		}
	}
	return items, errs
}

func (rs *ReplGroupStore) ReadGroup(parentKeyA, parentKeyB uint64) ([]store.ReadGroupItem, error) {
	type rettype struct {
		items []store.ReadGroupItem
		err   ReplGroupStoreError
	}
	ec := make(chan *rettype)
	stores := rs.storesFor(parentKeyA)
	for _, s := range stores {
		if s == nil {
			continue
		}
		go func(s store.GroupStore) {
			items, err := s.ReadGroup(parentKeyA, parentKeyB)
			ret := &rettype{items: items}
			if err != nil {
				ret.err = &replGroupStoreError{store: s, err: err}
			}
			ec <- ret
		}(s)
	}
	var items []store.ReadGroupItem
	var errs ReplGroupStoreErrorSlice
	// TODO: Selection algorithms
	for _, s := range stores {
		if s == nil {
			errs = append(errs, nilGroupStoreErr)
			continue
		}
		ret := <-ec
		if ret.err != nil {
			errs = append(errs, ret.err)
		} else if len(ret.items) > len(items) {
			items = ret.items
		}
	}
	return items, errs
}

type ReplGroupStoreError interface {
	error
	Store() store.GroupStore
	Err() error
}

type ReplGroupStoreErrorSlice []ReplGroupStoreError

func (es ReplGroupStoreErrorSlice) Error() string {
	if len(es) <= 0 {
		return "unknown error"
	} else if len(es) == 1 {
		return es[0].Error()
	}
	return fmt.Sprintf("%d errors, first is: %s", len(es), es[0])
}

type ReplGroupStoreErrorNotFound ReplGroupStoreErrorSlice

func (e ReplGroupStoreErrorNotFound) Error() string {
	if len(e) <= 0 {
		return "unknown error"
	} else if len(e) == 1 {
		return e[0].Error()
	}
	return fmt.Sprintf("%d errors, first is: %s", len(e), e[0])
}

func (e ReplGroupStoreErrorNotFound) ErrorNotFound() string {
	return e.Error()
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

func (e *replGroupStoreError) Store() store.GroupStore {
	return e.store
}

func (e *replGroupStoreError) Err() error {
	return e.err
}

var nilGroupStoreErr = &replGroupStoreError{err: errors.New("nil store")}
