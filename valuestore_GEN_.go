package replstore

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gholt/ring"
	"github.com/gholt/store"
	"github.com/pandemicsyn/oort/api"
	"golang.org/x/net/context"
)

type ReplValueStore struct {
	addressIndex int

	ringLock sync.RWMutex
	ring     ring.Ring

	storesLock sync.RWMutex
	stores     map[string]*replValueStoreAndTicketChan
}

type replValueStoreAndTicketChan struct {
	store      store.ValueStore
	ticketChan chan struct{}
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
	nodes := r.Nodes()
	currentAddrs := make(map[string]struct{}, len(nodes))
	for _, n := range nodes {
		currentAddrs[n.Address(rs.addressIndex)] = struct{}{}
	}
	var shutdownAddrs []string
	rs.storesLock.RLock()
	for a := range rs.stores {
		if _, ok := currentAddrs[a]; !ok {
			shutdownAddrs = append(shutdownAddrs, a)
		}
	}
	rs.storesLock.RUnlock()
	if len(shutdownAddrs) > 0 {
		shutdownStores := make([]*replValueStoreAndTicketChan, len(shutdownAddrs))
		rs.storesLock.Lock()
		for i, a := range shutdownAddrs {
			shutdownStores[i] = rs.stores[a]
			rs.stores[a] = nil
		}
		rs.storesLock.Unlock()
		for _, s := range shutdownStores {
			if err := s.store.Shutdown(context.Background()); err != nil {
				// TODO: debug log the err for completeness
			}
		}
	}
}

func (rs *ReplValueStore) storesFor(ctx context.Context, keyA uint64) []*replValueStoreAndTicketChan {
	// TODO: Pay attention to ctx.
	rs.ringLock.RLock()
	r := rs.ring
	rs.ringLock.RUnlock()
	ns := r.ResponsibleNodes(uint32(keyA >> (64 - r.PartitionBitCount())))
	as := make([]string, len(ns))
	for i, n := range ns {
		as[i] = n.Address(rs.addressIndex)
	}
	ss := make([]*replValueStoreAndTicketChan, len(ns))
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
					// TODO: The 10 here is arbitrary.
					tc := make(chan struct{}, 10)
					for i := cap(tc); i > 0; i-- {
						tc <- struct{}{}
					}
					ss[i] = &replValueStoreAndTicketChan{ticketChan: tc}
					// TODO: The 10 here is arbitrary (and unrelated to the
					// above arbitrary 10).
					ss[i].store, err = api.NewValueStore(ctx, as[i], 10)
					if err != nil {
						ss[i].store = errorValueStore(fmt.Sprintf("could not create store for %s: %s", as[i], err))
						go func(addr string) {
							rs.storesLock.Lock()
							s := rs.stores[addr]
							if s != nil {
								if _, ok := s.store.(errorValueStore); ok {
									rs.stores[addr] = nil
								}
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

func (rs *ReplValueStore) Lookup(ctx context.Context, keyA, keyB uint64) (int64, uint32, error) {
	// TODO: Pay attention to ctx.
	type rettype struct {
		timestampMicro int64
		length         uint32
		err            ReplValueStoreError
	}
	ec := make(chan *rettype)
	stores := rs.storesFor(ctx, keyA)
	for _, s := range stores {
		go func(s *replValueStoreAndTicketChan) {
			ret := &rettype{}
			var err error
			select {
			case <-s.ticketChan:
				ret.timestampMicro, ret.length, err = s.store.Lookup(ctx, keyA, keyB)
				s.ticketChan <- struct{}{}
			case <-time.After(time.Second):
				// TODO: That time.Second above is arbitrary.
				err = timeoutErr
			}
			if err != nil {
				ret.err = &replValueStoreError{store: s.store, err: err}
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

func (rs *ReplValueStore) Read(ctx context.Context, keyA uint64, keyB uint64, value []byte) (int64, []byte, error) {
	// TODO: Pay attention to ctx.
	type rettype struct {
		timestampMicro int64
		value          []byte
		err            ReplValueStoreError
	}
	ec := make(chan *rettype)
	stores := rs.storesFor(ctx, keyA)
	for _, s := range stores {
		go func(s *replValueStoreAndTicketChan) {
			ret := &rettype{}
			var err error
			select {
			case <-s.ticketChan:
				ret.timestampMicro, ret.value, err = s.store.Read(ctx, keyA, keyB, nil)
				s.ticketChan <- struct{}{}
			case <-time.After(time.Second):
				// TODO: That time.Second above is arbitrary.
				err = timeoutErr
			}
			if err != nil {
				ret.err = &replValueStoreError{store: s.store, err: err}
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
	if value != nil && rvalue != nil {
		rvalue = append(value, rvalue...)
	}
	if notFounds > 0 {
		nferrs := make(ReplValueStoreErrorNotFound, len(errs))
		for i, v := range errs {
			nferrs[i] = v
		}
		return timestampMicro, rvalue, nferrs
	}
	return timestampMicro, rvalue, errs
}

func (rs *ReplValueStore) Write(ctx context.Context, keyA uint64, keyB uint64, timestampMicro int64, value []byte) (int64, error) {
	// TODO: Pay attention to ctx.
	type rettype struct {
		oldTimestampMicro int64
		err               ReplValueStoreError
	}
	ec := make(chan *rettype)
	stores := rs.storesFor(ctx, keyA)
	for _, s := range stores {
		go func(s *replValueStoreAndTicketChan) {
			ret := &rettype{}
			var err error
			select {
			case <-s.ticketChan:
				ret.oldTimestampMicro, err = s.store.Write(ctx, keyA, keyB, timestampMicro, value)
				s.ticketChan <- struct{}{}
			case <-time.After(time.Second):
				// TODO: That time.Second above is arbitrary.
				err = timeoutErr
			}
			if err != nil {
				ret.err = &replValueStoreError{store: s.store, err: err}
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

func (rs *ReplValueStore) Delete(ctx context.Context, keyA uint64, keyB uint64, timestampMicro int64) (int64, error) {
	// TODO: Pay attention to ctx.
	type rettype struct {
		oldTimestampMicro int64
		err               ReplValueStoreError
	}
	ec := make(chan *rettype)
	stores := rs.storesFor(ctx, keyA)
	for _, s := range stores {
		go func(s *replValueStoreAndTicketChan) {
			ret := &rettype{}
			var err error
			select {
			case <-s.ticketChan:
				ret.oldTimestampMicro, err = s.store.Delete(ctx, keyA, keyB, timestampMicro)
				s.ticketChan <- struct{}{}
			case <-time.After(time.Second):
				// TODO: That time.Second above is arbitrary.
				err = timeoutErr
			}
			if err != nil {
				ret.err = &replValueStoreError{store: s.store, err: err}
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
