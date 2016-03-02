package replstore

import (
	"fmt"
	"math"

	"github.com/gholt/store"
)

type ReplGroupStore struct {
	Stores []store.GroupStore
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

func (rs *ReplGroupStore) helper(f func(s store.GroupStore) error) error {
	ec := make(chan ReplGroupStoreError)
	for _, s := range rs.Stores {
		go func(s store.GroupStore) {
			if err := f(s); err != nil {
				ec <- &replGroupStoreError{store: s, err: err}
			} else {
				ec <- nil
			}
		}(s)
	}
	var errs ReplGroupStoreErrorSlice
	for _ = range rs.Stores {
		if err := <-ec; err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errs
	}
	return nil
}

func (rs *ReplGroupStore) Startup() error {
	return rs.helper(func(s store.GroupStore) error { return s.Startup() })
}

func (rs *ReplGroupStore) Shutdown() error {
	return rs.helper(func(s store.GroupStore) error { return s.Shutdown() })
}

func (rs *ReplGroupStore) EnableWrites() error {
	return rs.helper(func(s store.GroupStore) error { return s.EnableWrites() })
}

func (rs *ReplGroupStore) DisableWrites() error {
	return rs.helper(func(s store.GroupStore) error { return s.DisableWrites() })
}

func (rs *ReplGroupStore) Flush() error {
	return rs.helper(func(s store.GroupStore) error { return s.Flush() })
}

func (rs *ReplGroupStore) AuditPass() error {
	return rs.helper(func(s store.GroupStore) error { return s.AuditPass() })
}

type ReplGroupStoreStats interface {
	fmt.Stringer
	Store() store.GroupStore
	Stats() fmt.Stringer
}

type replGroupStoreStats struct {
	store store.GroupStore
	stats fmt.Stringer
}

func (s *replGroupStoreStats) Store() store.GroupStore {
	return s.store
}

func (s *replGroupStoreStats) Stats() fmt.Stringer {
	return s.stats
}

func (s *replGroupStoreStats) String() string {
	if s.stats == nil {
		return "nil stats"
	}
	return s.stats.String()
}

type ReplGroupStoreStatsSlice []ReplGroupStoreStats

func (ss ReplGroupStoreStatsSlice) String() string {
	if len(ss) <= 0 {
		return "nil stats"
	} else if len(ss) == 1 {
		return ss[0].String()
	}
	return fmt.Sprintf("%d stats, first is: %s", len(ss), ss[0])
}

func (rs *ReplGroupStore) Stats(debug bool) (fmt.Stringer, error) {
	type rettype struct {
		stats ReplGroupStoreStats
		err   ReplGroupStoreError
	}
	retchan := make(chan *rettype)
	for _, s := range rs.Stores {
		go func(s store.GroupStore) {
			stats, err := s.Stats(debug)
			ret := &rettype{}
			if stats != nil {
				ret.stats = &replGroupStoreStats{store: s, stats: stats}
			}
			if err != nil {
				ret.err = &replGroupStoreError{store: s, err: err}
			}
			retchan <- ret
		}(s)
	}
	var stats ReplGroupStoreStatsSlice
	var errs ReplGroupStoreErrorSlice
	for _ = range rs.Stores {
		ret := <-retchan
		if ret.stats != nil {
			stats = append(stats, ret.stats)
		}
		if ret.err != nil {
			errs = append(errs, ret.err)
		}
	}
	return stats, errs
}

func (rs *ReplGroupStore) ValueCap() (uint32, error) {
	type rettype struct {
		vcap uint32
		err  ReplGroupStoreError
	}
	ec := make(chan *rettype)
	for _, s := range rs.Stores {
		go func(s store.GroupStore) {
			vcap, err := s.ValueCap()
			if err != nil {
				ec <- &rettype{
					vcap: vcap,
					err:  &replGroupStoreError{store: s, err: err},
				}
			} else {
				ec <- &rettype{vcap: vcap}
			}
		}(s)
	}
	vcap := uint32(math.MaxUint32)
	var errs ReplGroupStoreErrorSlice
	for _ = range rs.Stores {
		ret := <-ec
		if ret.err != nil {
			errs = append(errs, ret.err)
		} else if ret.vcap < vcap {
			vcap = ret.vcap
		}
	}
	if len(errs) > 0 {
		return 0, errs
	}
	return vcap, nil
}

func (rs *ReplGroupStore) Lookup(keyA, keyB uint64, childKeyA, childKeyB uint64) (int64, uint32, error) {
	type rettype struct {
		timestampMicro int64
		length         uint32
		err            ReplGroupStoreError
	}
	ec := make(chan *rettype)
	for _, s := range rs.Stores {
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
	for _ = range rs.Stores {
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
	for _, s := range rs.Stores {
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
	for _ = range rs.Stores {
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
	for _, s := range rs.Stores {
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
	for _ = range rs.Stores {
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
	for _, s := range rs.Stores {
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
	for _ = range rs.Stores {
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
	for _, s := range rs.Stores {
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
	for _ = range rs.Stores {
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
	for _, s := range rs.Stores {
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
	for _ = range rs.Stores {
		ret := <-ec
		if ret.err != nil {
			errs = append(errs, ret.err)
		} else if len(ret.items) > len(items) {
			items = ret.items
		}
	}
	return items, errs
}
