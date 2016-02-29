package replstore

import (
	"fmt"
	"math"

	"github.com/gholt/store"
)

type ReplValueStore struct {
	Stores []store.ValueStore
}

type ReplValueStoreError struct {
	Store store.ValueStore
	Err   error
}

func (e *ReplValueStoreError) Error() string {
	if e.Err == nil {
		return "unknown error"
	}
	return e.Err.Error()
}

type ReplValueStoreErrorSlice []*ReplValueStoreError

func (es ReplValueStoreErrorSlice) Error() string {
	if len(es) <= 0 {
		return "unknown error"
	} else if len(es) == 1 {
		return es[0].Error()
	}
	return fmt.Sprintf("%d errors, first is: %s", len(es), es[0])
}

func (rs *ReplValueStore) helper(f func(s store.ValueStore) error) error {
	ec := make(chan *ReplValueStoreError)
	for _, s := range rs.Stores {
		go func(s store.ValueStore) {
			if err := f(s); err != nil {
				ec <- &ReplValueStoreError{Store: s, Err: err}
			} else {
				ec <- nil
			}
		}(s)
	}
	var errs ReplValueStoreErrorSlice
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

func (rs *ReplValueStore) Startup() error {
	return rs.helper(func(s store.ValueStore) error { return s.Startup() })
}

func (rs *ReplValueStore) Shutdown() error {
	return rs.helper(func(s store.ValueStore) error { return s.Shutdown() })
}

func (rs *ReplValueStore) EnableWrites() error {
	return rs.helper(func(s store.ValueStore) error { return s.EnableWrites() })
}

func (rs *ReplValueStore) DisableWrites() error {
	return rs.helper(func(s store.ValueStore) error { return s.DisableWrites() })
}

func (rs *ReplValueStore) Flush() error {
	return rs.helper(func(s store.ValueStore) error { return s.Flush() })
}

func (rs *ReplValueStore) AuditPass() error {
	return rs.helper(func(s store.ValueStore) error { return s.AuditPass() })
}

type ReplValueStoreStats struct {
	Store store.ValueStore
	Stats fmt.Stringer
}

func (s *ReplValueStoreStats) String() string {
	if s.Stats == nil {
		return "nil stats"
	}
	return s.Stats.String()
}

type ReplValueStoreStatsSlice []*ReplValueStoreStats

func (ss ReplValueStoreStatsSlice) String() string {
	if len(ss) <= 0 {
		return "nil stats"
	} else if len(ss) == 1 {
		return ss[0].String()
	}
	return fmt.Sprintf("%d stats, first is: %s", len(ss), ss[0])
}

func (rs *ReplValueStore) Stats(debug bool) (fmt.Stringer, error) {
	type rettype struct {
		stats *ReplValueStoreStats
		err   *ReplValueStoreError
	}
	retchan := make(chan *rettype)
	for _, s := range rs.Stores {
		go func(s store.ValueStore) {
			stats, err := s.Stats(debug)
			ret := &rettype{}
			if stats != nil {
				ret.stats = &ReplValueStoreStats{Store: s, Stats: stats}
			}
			if err != nil {
				ret.err = &ReplValueStoreError{Store: s, Err: err}
			}
			retchan <- ret
		}(s)
	}
	var stats ReplValueStoreStatsSlice
	var errs ReplValueStoreErrorSlice
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

func (rs *ReplValueStore) ValueCap() (uint32, error) {
	type rettype struct {
		vcap uint32
		err  *ReplValueStoreError
	}
	ec := make(chan *rettype)
	for _, s := range rs.Stores {
		go func(s store.ValueStore) {
			vcap, err := s.ValueCap()
			if err != nil {
				ec <- &rettype{
					vcap: vcap,
					err:  &ReplValueStoreError{Store: s, Err: err},
				}
			} else {
				ec <- &rettype{vcap: vcap}
			}
		}(s)
	}
	vcap := uint32(math.MaxUint32)
	var errs ReplValueStoreErrorSlice
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

func (rs *ReplValueStore) Lookup(keyA, keyB uint64) (int64, uint32, error) {
	type rettype struct {
		timestampMicro int64
		length         uint32
		err            *ReplValueStoreError
	}
	ec := make(chan *rettype)
	for _, s := range rs.Stores {
		go func(s store.ValueStore) {
			timestampMicro, length, err := s.Lookup(keyA, keyB)
			ret := &rettype{timestampMicro: timestampMicro, length: length}
			if err != nil {
				ret.err = &ReplValueStoreError{Store: s, Err: err}
			}
			ec <- ret
		}(s)
	}
	var timestampMicro int64
	var length uint32
	var errs ReplValueStoreErrorSlice
	// TODO: Selection algorithms
	for _ = range rs.Stores {
		ret := <-ec
		if ret.err != nil {
			errs = append(errs, ret.err)
		} else if ret.timestampMicro > timestampMicro {
			timestampMicro = ret.timestampMicro
			length = ret.length
		}
	}
	return timestampMicro, length, errs
}

func (rs *ReplValueStore) Read(keyA uint64, keyB uint64, value []byte) (int64, []byte, error) {
	type rettype struct {
		timestampMicro int64
		value          []byte
		err            *ReplValueStoreError
	}
	ec := make(chan *rettype)
	for _, s := range rs.Stores {
		go func(s store.ValueStore) {
			timestampMicro, value, err := s.Read(keyA, keyB, nil)
			ret := &rettype{timestampMicro: timestampMicro, value: value}
			if err != nil {
				ret.err = &ReplValueStoreError{Store: s, Err: err}
			}
			ec <- ret
		}(s)
	}
	var timestampMicro int64
	var rvalue []byte
	var errs ReplValueStoreErrorSlice
	// TODO: Selection algorithms
	for _ = range rs.Stores {
		ret := <-ec
		if ret.err != nil {
			errs = append(errs, ret.err)
		} else if ret.timestampMicro > timestampMicro {
			timestampMicro = ret.timestampMicro
			rvalue = ret.value
		}
	}
	return timestampMicro, append(value, rvalue...), errs
}

func (rs *ReplValueStore) Write(keyA uint64, keyB uint64, timestampMicro int64, value []byte) (int64, error) {
	type rettype struct {
		oldTimestampMicro int64
		err               *ReplValueStoreError
	}
	ec := make(chan *rettype)
	for _, s := range rs.Stores {
		go func(s store.ValueStore) {
			oldTimestampMicro, err := s.Write(keyA, keyB, timestampMicro, value)
			ret := &rettype{oldTimestampMicro: oldTimestampMicro}
			if err != nil {
				ret.err = &ReplValueStoreError{Store: s, Err: err}
			}
			ec <- ret
		}(s)
	}
	var oldTimestampMicro int64
	var errs ReplValueStoreErrorSlice
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

func (rs *ReplValueStore) Delete(keyA uint64, keyB uint64, timestampMicro int64) (int64, error) {
	type rettype struct {
		oldTimestampMicro int64
		err               *ReplValueStoreError
	}
	ec := make(chan *rettype)
	for _, s := range rs.Stores {
		go func(s store.ValueStore) {
			oldTimestampMicro, err := s.Delete(keyA, keyB, timestampMicro)
			ret := &rettype{oldTimestampMicro: oldTimestampMicro}
			if err != nil {
				ret.err = &ReplValueStoreError{Store: s, Err: err}
			}
			ec <- ret
		}(s)
	}
	var oldTimestampMicro int64
	var errs ReplValueStoreErrorSlice
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
