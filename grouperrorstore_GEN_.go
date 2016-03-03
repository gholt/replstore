package replstore

import (
	"fmt"

	"github.com/gholt/store"
)

type errorGroupStore string

func (es errorGroupStore) String() string {
	return string(es)
}

func (es errorGroupStore) Error() string {
	return string(es)
}

func (es errorGroupStore) Startup() error {
	return es
}

func (es errorGroupStore) Shutdown() error {
	return es
}

func (es errorGroupStore) EnableWrites() error {
	return es
}

func (es errorGroupStore) DisableWrites() error {
	return es
}

func (es errorGroupStore) Flush() error {
	return es
}

func (es errorGroupStore) AuditPass() error {
	return es
}

func (es errorGroupStore) Stats(debug bool) (fmt.Stringer, error) {
	return es, es
}

func (es errorGroupStore) ValueCap() (uint32, error) {
	return 0, es
}

func (es errorGroupStore) Lookup(keyA uint64, keyB uint64, childKeyA uint64, childKeyB uint64) (int64, uint32, error) {
	return 0, 0, es
}

func (es errorGroupStore) Read(keyA uint64, keyB uint64, childKeyA uint64, childKeyB uint64, value []byte) (int64, []byte, error) {
	return 0, value, es
}

func (es errorGroupStore) Write(keyA uint64, keyB uint64, childKeyA uint64, childKeyB uint64, timestampMicro int64, value []byte) (int64, error) {
	return 0, es
}

func (es errorGroupStore) Delete(keyA uint64, keyB uint64, childKeyA uint64, childKeyB uint64, timestampMicro int64) (int64, error) {
	return 0, es
}

func (es errorGroupStore) LookupGroup(parentKeyA, parentKeyB uint64) ([]store.LookupGroupItem, error) {
	return nil, es
}

func (es errorGroupStore) ReadGroup(parentKeyA, parentKeyB uint64) ([]store.ReadGroupItem, error) {
	return nil, es
}
