package replstore

import (
	"errors"
	"fmt"
	"sync"

	"github.com/gholt/ring"
	"github.com/gholt/store"
	"github.com/pandemicsyn/oort/api"
)

type ReplValueStore struct {
	addressIndex int

	ringLock sync.RWMutex
	ring     ring.Ring

	storesLock sync.RWMutex
	stores     map[string]store.ValueStore
}

func NewReplValueStore(c *ReplValueStoreConfig) *ReplValueStore {
	cfg := resolveReplValueStoreConfig(c)
	return &ReplValueStore{addressIndex: cfg.AddressIndex}
}

func (rs *ReplValueStore) Ring() ring.Ring {
	rs.ringLock.RLock()
	r := rs.ring
	rs.ringLock.RUnlock()
	return r
}

func (rs *ReplValueStore) SetRing(r ring.Ring) {
	rs.ringLock.Lock()
	rs.ring = r
	rs.ringLock.Unlock()
	// TODO: Scan through the rs.stores map and discard no longer referenced
	// stores.
}

func (rs *ReplValueStore) storesFor(keyA uint64) []store.ValueStore {
	rs.ringLock.RLock()
	r := rs.ring
	rs.ringLock.RUnlock()
	ns := r.ResponsibleNodes(uint32(keyA >> (64 - r.PartitionBitCount())))
	as := make([]string, len(ns))
	for i, n := range ns {
		as[i] = n.Address(rs.addressIndex)
	}
	ss := make([]store.ValueStore, len(ns))
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
					ss[i], err = api.NewValueStore(as[i], 10)
					if err != nil {
						ss[i] = errorValueStore(fmt.Sprintf("could not create store for %s: %s", as[i], err))
						go func(addr string) {
							rs.storesLock.Lock()
							if _, ok := rs.stores[addr]; ok {
								rs.stores[addr] = nil
							}
							rs.storesLock.Unlock()
						}(as[i])
					}
					rs.stores[as[i]] = ss[i]
				}
			}
		}
		rs.storesLock.Unlock()
	}
	return ss
}

func (rs *ReplValueStore) Startup() error {
	return nil
}

func (rs *ReplValueStore) Shutdown() error {
	return nil
}

func (rs *ReplValueStore) EnableWrites() error {
	return nil
}

func (rs *ReplValueStore) DisableWrites() error {
	return errors.New("cannot disable writes with this client at this time")
}

func (rs *ReplValueStore) Flush() error {
	return nil
}

func (rs *ReplValueStore) AuditPass() error {
	return errors.New("audit passes not available with this client at this time")
}

func (rs *ReplValueStore) Stats(debug bool) (fmt.Stringer, error) {
	return noStats, nil
}

func (rs *ReplValueStore) ValueCap() (uint32, error) {
	return 0xffffffff, nil
}

func (rs *ReplValueStore) Lookup(keyA, keyB uint64) (int64, uint32, error) {
	type rettype struct {
		timestampMicro int64
		length         uint32
		err            ReplValueStoreError
	}
	ec := make(chan *rettype)
	stores := rs.storesFor(keyA)
	for _, s := range stores {
		go func(s store.ValueStore) {
			timestampMicro, length, err := s.Lookup(keyA, keyB)
			ret := &rettype{timestampMicro: timestampMicro, length: length}
			if err != nil {
				ret.err = &replValueStoreError{store: s, err: err}
			}
			ec <- ret
		}(s)
	}
	var timestampMicro int64
	var length uint32
	var errs ReplValueStoreErrorSlice
	var notFounds int
	// TODO: Selection algorithms
	for _ = range stores {
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
		nferrs := make(ReplValueStoreErrorNotFound, len(errs))
		for i, v := range errs {
			nferrs[i] = v
		}
		return timestampMicro, length, nferrs
	}
	return timestampMicro, length, errs
}

func (rs *ReplValueStore) Read(keyA uint64, keyB uint64, value []byte) (int64, []byte, error) {
	type rettype struct {
		timestampMicro int64
		value          []byte
		err            ReplValueStoreError
	}
	ec := make(chan *rettype)
	stores := rs.storesFor(keyA)
	for _, s := range stores {
		go func(s store.ValueStore) {
			timestampMicro, value, err := s.Read(keyA, keyB, nil)
			ret := &rettype{timestampMicro: timestampMicro, value: value}
			if err != nil {
				ret.err = &replValueStoreError{store: s, err: err}
			}
			ec <- ret
		}(s)
	}
	var timestampMicro int64
	var rvalue []byte
	var errs ReplValueStoreErrorSlice
	var notFounds int
	// TODO: Selection algorithms
	for _ = range stores {
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
		nferrs := make(ReplValueStoreErrorNotFound, len(errs))
		for i, v := range errs {
			nferrs[i] = v
		}
		return timestampMicro, append(value, rvalue...), nferrs
	}
	return timestampMicro, append(value, rvalue...), errs
}

func (rs *ReplValueStore) Write(keyA uint64, keyB uint64, timestampMicro int64, value []byte) (int64, error) {
	type rettype struct {
		oldTimestampMicro int64
		err               ReplValueStoreError
	}
	ec := make(chan *rettype)
	stores := rs.storesFor(keyA)
	for _, s := range stores {
		go func(s store.ValueStore) {
			oldTimestampMicro, err := s.Write(keyA, keyB, timestampMicro, value)
			ret := &rettype{oldTimestampMicro: oldTimestampMicro}
			if err != nil {
				ret.err = &replValueStoreError{store: s, err: err}
			}
			ec <- ret
		}(s)
	}
	var oldTimestampMicro int64
	var errs ReplValueStoreErrorSlice
	// TODO: Selection algorithms
	for _ = range stores {
		ret := <-ec
		if ret.err != nil {
			errs = append(errs, ret.err)
		} else if ret.oldTimestampMicro > oldTimestampMicro {
			oldTimestampMicro = ret.oldTimestampMicro
		}
	}
	return oldTimestampMicro, errs
}

func (rs *ReplValueStore) Delete(keyA uint64, keyB uint64, timestampMicro int64) (int64, error) {
	type rettype struct {
		oldTimestampMicro int64
		err               ReplValueStoreError
	}
	ec := make(chan *rettype)
	stores := rs.storesFor(keyA)
	for _, s := range stores {
		go func(s store.ValueStore) {
			oldTimestampMicro, err := s.Delete(keyA, keyB, timestampMicro)
			ret := &rettype{oldTimestampMicro: oldTimestampMicro}
			if err != nil {
				ret.err = &replValueStoreError{store: s, err: err}
			}
			ec <- ret
		}(s)
	}
	var oldTimestampMicro int64
	var errs ReplValueStoreErrorSlice
	// TODO: Selection algorithms
	for _ = range stores {
		ret := <-ec
		if ret.err != nil {
			errs = append(errs, ret.err)
		} else if ret.oldTimestampMicro > oldTimestampMicro {
			oldTimestampMicro = ret.oldTimestampMicro
		}
	}
	return oldTimestampMicro, errs
}

type ReplValueStoreError interface {
	error
	Store() store.ValueStore
	Err() error
}

type ReplValueStoreErrorSlice []ReplValueStoreError

func (es ReplValueStoreErrorSlice) Error() string {
	if len(es) <= 0 {
		return "unknown error"
	} else if len(es) == 1 {
		return es[0].Error()
	}
	return fmt.Sprintf("%d errors, first is: %s", len(es), es[0])
}

type ReplValueStoreErrorNotFound ReplValueStoreErrorSlice

func (e ReplValueStoreErrorNotFound) Error() string {
	if len(e) <= 0 {
		return "unknown error"
	} else if len(e) == 1 {
		return e[0].Error()
	}
	return fmt.Sprintf("%d errors, first is: %s", len(e), e[0])
}

func (e ReplValueStoreErrorNotFound) ErrorNotFound() string {
	return e.Error()
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

func (e *replValueStoreError) Store() store.ValueStore {
	return e.store
}

func (e *replValueStoreError) Err() error {
	return e.err
}
