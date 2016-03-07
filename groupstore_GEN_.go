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

type ReplGroupStore struct {
	addressIndex int

	ringLock sync.RWMutex
	ring     ring.Ring

	storesLock sync.RWMutex
	stores     map[string]*replGroupStoreAndTicketChan
}

type replGroupStoreAndTicketChan struct {
	store      store.GroupStore
	ticketChan chan struct{}
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
	var currentAddrs map[string]struct{}
	if r != nil {
		nodes := r.Nodes()
		currentAddrs = make(map[string]struct{}, len(nodes))
		for _, n := range nodes {
			currentAddrs[n.Address(rs.addressIndex)] = struct{}{}
		}
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
		shutdownStores := make([]*replGroupStoreAndTicketChan, len(shutdownAddrs))
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

func (rs *ReplGroupStore) storesFor(ctx context.Context, keyA uint64) ([]*replGroupStoreAndTicketChan, error) {
	rs.ringLock.RLock()
	r := rs.ring
	rs.ringLock.RUnlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if r == nil {
		return nil, noRingErr
	}
	ns := r.ResponsibleNodes(uint32(keyA >> (64 - r.PartitionBitCount())))
	as := make([]string, len(ns))
	for i, n := range ns {
		as[i] = n.Address(rs.addressIndex)
	}
	ss := make([]*replGroupStoreAndTicketChan, len(ns))
	var someNil bool
	rs.storesLock.RLock()
	for i := len(ss) - 1; i >= 0; i-- {
		ss[i] = rs.stores[as[i]]
		if ss[i] == nil {
			someNil = true
		}
	}
	rs.storesLock.RUnlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if someNil {
		rs.storesLock.Lock()
		select {
		case <-ctx.Done():
			rs.storesLock.Unlock()
			return nil, ctx.Err()
		default:
		}
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
					ss[i] = &replGroupStoreAndTicketChan{ticketChan: tc}
					// TODO: The 10 here is arbitrary (and unrelated to the
					// above arbitrary 10).
					ss[i].store, err = api.NewGroupStore(ctx, as[i], 10)
					if err != nil {
						ss[i].store = errorGroupStore(fmt.Sprintf("could not create store for %s: %s", as[i], err))
						// Launch goroutine to clear out the error store after
						// some time so a retry will occur.
						go func(addr string) {
							// TODO: The 15 seconds is arbitrary.
							time.Sleep(15 * time.Second)
							rs.storesLock.Lock()
							s := rs.stores[addr]
							if s != nil {
								if _, ok := s.store.(errorGroupStore); ok {
									rs.stores[addr] = nil
								}
							}
							rs.storesLock.Unlock()
						}(as[i])
					}
					rs.stores[as[i]] = ss[i]
					select {
					case <-ctx.Done():
						rs.storesLock.Unlock()
						return nil, ctx.Err()
					default:
					}
				}
			}
		}
		rs.storesLock.Unlock()
	}
	return ss, nil
}

func (rs *ReplGroupStore) Startup(ctx context.Context) error {
	return nil
}

func (rs *ReplGroupStore) Shutdown(ctx context.Context) error {
	return nil
}

func (rs *ReplGroupStore) EnableWrites(ctx context.Context) error {
	return nil
}

func (rs *ReplGroupStore) DisableWrites(ctx context.Context) error {
	return errors.New("cannot disable writes with this client at this time")
}

func (rs *ReplGroupStore) Flush(ctx context.Context) error {
	return nil
}

func (rs *ReplGroupStore) AuditPass(ctx context.Context) error {
	return errors.New("audit passes not available with this client at this time")
}

func (rs *ReplGroupStore) Stats(ctx context.Context, debug bool) (fmt.Stringer, error) {
	return noStats, nil
}

func (rs *ReplGroupStore) ValueCap(ctx context.Context) (uint32, error) {
	// TODO: Make this a config option? I doubt it's worth asking all available
	// nodes what their caps are in order to report the lowest.
	return 0xffffffff, nil
}

func (rs *ReplGroupStore) Lookup(ctx context.Context, keyA, keyB uint64, childKeyA, childKeyB uint64) (int64, uint32, error) {
	type rettype struct {
		timestampMicro int64
		length         uint32
		err            ReplGroupStoreError
	}
	ec := make(chan *rettype)
	stores, err := rs.storesFor(ctx, keyA)
	if err != nil {
		return 0, 0, err
	}
	for _, s := range stores {
		go func(s *replGroupStoreAndTicketChan) {
			ret := &rettype{}
			var err error
			select {
			case <-s.ticketChan:
				ret.timestampMicro, ret.length, err = s.store.Lookup(ctx, keyA, keyB, childKeyA, childKeyB)
				s.ticketChan <- struct{}{}
			case <-ctx.Done():
				err = ctx.Err()
			}
			if err != nil {
				ret.err = &replGroupStoreError{store: s.store, err: err}
			}
			ec <- ret
		}(s)
	}
	var timestampMicro int64
	var length uint32
	var notFound bool
	var errs ReplGroupStoreErrorSlice
	for _ = range stores {
		ret := <-ec
		if ret.timestampMicro > timestampMicro {
			timestampMicro = ret.timestampMicro
			length = ret.length
			notFound = store.IsNotFound(ret.err)
		}
		if ret.err != nil {
			errs = append(errs, ret.err)
		}
	}
	if notFound {
		nferrs := make(ReplGroupStoreErrorNotFound, len(errs))
		for i, v := range errs {
			nferrs[i] = v
		}
		return timestampMicro, length, nferrs
	}
	if len(errs) < len(stores) {
		// TODO: Debug log these errors.
		errs = nil
	}
	return timestampMicro, length, errs
}

func (rs *ReplGroupStore) Read(ctx context.Context, keyA uint64, keyB uint64, childKeyA, childKeyB uint64, value []byte) (int64, []byte, error) {
	type rettype struct {
		timestampMicro int64
		value          []byte
		err            ReplGroupStoreError
	}
	ec := make(chan *rettype)
	stores, err := rs.storesFor(ctx, keyA)
	if err != nil {
		return 0, nil, err
	}
	for _, s := range stores {
		go func(s *replGroupStoreAndTicketChan) {
			ret := &rettype{}
			var err error
			select {
			case <-s.ticketChan:
				ret.timestampMicro, ret.value, err = s.store.Read(ctx, keyA, keyB, childKeyA, childKeyB, nil)
				s.ticketChan <- struct{}{}
			case <-ctx.Done():
				err = ctx.Err()
			}
			if err != nil {
				ret.err = &replGroupStoreError{store: s.store, err: err}
			}
			ec <- ret
		}(s)
	}
	var timestampMicro int64
	var rvalue []byte
	var notFound bool
	var errs ReplGroupStoreErrorSlice
	for _ = range stores {
		ret := <-ec
		if ret.timestampMicro > timestampMicro {
			timestampMicro = ret.timestampMicro
			rvalue = ret.value
			notFound = store.IsNotFound(ret.err)
		}
		if ret.err != nil {
			errs = append(errs, ret.err)
		}
	}
	if value != nil && rvalue != nil {
		rvalue = append(value, rvalue...)
	}
	if notFound {
		nferrs := make(ReplGroupStoreErrorNotFound, len(errs))
		for i, v := range errs {
			nferrs[i] = v
		}
		return timestampMicro, rvalue, nferrs
	}
	if len(errs) < len(stores) {
		// TODO: Debug log these errors.
		errs = nil
	}
	return timestampMicro, rvalue, errs
}

func (rs *ReplGroupStore) Write(ctx context.Context, keyA uint64, keyB uint64, childKeyA, childKeyB uint64, timestampMicro int64, value []byte) (int64, error) {
	type rettype struct {
		oldTimestampMicro int64
		err               ReplGroupStoreError
	}
	ec := make(chan *rettype)
	stores, err := rs.storesFor(ctx, keyA)
	if err != nil {
		return 0, err
	}
	for _, s := range stores {
		go func(s *replGroupStoreAndTicketChan) {
			ret := &rettype{}
			var err error
			select {
			case <-s.ticketChan:
				ret.oldTimestampMicro, err = s.store.Write(ctx, keyA, keyB, childKeyA, childKeyB, timestampMicro, value)
				s.ticketChan <- struct{}{}
			case <-ctx.Done():
				err = ctx.Err()
			}
			if err != nil {
				ret.err = &replGroupStoreError{store: s.store, err: err}
			}
			ec <- ret
		}(s)
	}
	var oldTimestampMicro int64
	var errs ReplGroupStoreErrorSlice
	for _ = range stores {
		ret := <-ec
		if ret.err != nil {
			errs = append(errs, ret.err)
		} else if ret.oldTimestampMicro > oldTimestampMicro {
			oldTimestampMicro = ret.oldTimestampMicro
		}
	}
	if len(errs) < (len(stores)+1)/2 {
		// TODO: Debug log these errors.
		errs = nil
	}
	return oldTimestampMicro, errs
}

func (rs *ReplGroupStore) Delete(ctx context.Context, keyA uint64, keyB uint64, childKeyA, childKeyB uint64, timestampMicro int64) (int64, error) {
	type rettype struct {
		oldTimestampMicro int64
		err               ReplGroupStoreError
	}
	ec := make(chan *rettype)
	stores, err := rs.storesFor(ctx, keyA)
	if err != nil {
		return 0, err
	}
	for _, s := range stores {
		go func(s *replGroupStoreAndTicketChan) {
			ret := &rettype{}
			var err error
			select {
			case <-s.ticketChan:
				ret.oldTimestampMicro, err = s.store.Delete(ctx, keyA, keyB, childKeyA, childKeyB, timestampMicro)
				s.ticketChan <- struct{}{}
			case <-ctx.Done():
				err = ctx.Err()
			}
			if err != nil {
				ret.err = &replGroupStoreError{store: s.store, err: err}
			}
			ec <- ret
		}(s)
	}
	var oldTimestampMicro int64
	var errs ReplGroupStoreErrorSlice
	for _ = range stores {
		ret := <-ec
		if ret.err != nil {
			errs = append(errs, ret.err)
		} else if ret.oldTimestampMicro > oldTimestampMicro {
			oldTimestampMicro = ret.oldTimestampMicro
		}
	}
	if len(errs) < (len(stores)+1)/2 {
		// TODO: Debug log these errors.
		errs = nil
	}
	return oldTimestampMicro, errs
}

func (rs *ReplGroupStore) LookupGroup(ctx context.Context, parentKeyA, parentKeyB uint64) ([]store.LookupGroupItem, error) {
	type rettype struct {
		items []store.LookupGroupItem
		err   ReplGroupStoreError
	}
	ec := make(chan *rettype)
	stores, err := rs.storesFor(ctx, parentKeyA)
	if err != nil {
		return nil, err
	}
	for _, s := range stores {
		go func(s *replGroupStoreAndTicketChan) {
			ret := &rettype{}
			var err error
			select {
			case <-s.ticketChan:
				ret.items, err = s.store.LookupGroup(ctx, parentKeyA, parentKeyB)
				s.ticketChan <- struct{}{}
			case <-ctx.Done():
				err = ctx.Err()
			}
			if err != nil {
				ret.err = &replGroupStoreError{store: s.store, err: err}
			}
			ec <- ret
		}(s)
	}
	var items []store.LookupGroupItem
	var errs ReplGroupStoreErrorSlice
	for _ = range stores {
		ret := <-ec
		if ret.err != nil {
			errs = append(errs, ret.err)
		} else if len(ret.items) > len(items) {
			items = ret.items
		}
	}
	if len(errs) == len(stores) {
		return items, errs
	} // else debug log other errs
	return items, nil
}

func (rs *ReplGroupStore) ReadGroup(ctx context.Context, parentKeyA, parentKeyB uint64) ([]store.ReadGroupItem, error) {
	type rettype struct {
		items []store.ReadGroupItem
		err   ReplGroupStoreError
	}
	ec := make(chan *rettype)
	stores, err := rs.storesFor(ctx, parentKeyA)
	if err != nil {
		return nil, err
	}
	for _, s := range stores {
		go func(s *replGroupStoreAndTicketChan) {
			ret := &rettype{}
			var err error
			select {
			case <-s.ticketChan:
				ret.items, err = s.store.ReadGroup(ctx, parentKeyA, parentKeyB)
				s.ticketChan <- struct{}{}
			case <-ctx.Done():
				err = ctx.Err()
			}
			if err != nil {
				ret.err = &replGroupStoreError{store: s.store, err: err}
			}
			ec <- ret
		}(s)
	}
	var items []store.ReadGroupItem
	var errs ReplGroupStoreErrorSlice
	for _ = range stores {
		ret := <-ec
		if ret.err != nil {
			errs = append(errs, ret.err)
		} else if len(ret.items) > len(items) {
			items = ret.items
		}
	}
	if len(errs) == len(stores) {
		return items, errs
	} // else debug log other errs
	return items, nil
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
