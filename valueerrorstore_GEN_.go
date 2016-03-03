package replstore

import (
	"fmt"
)

type errorValueStore string

func (es errorValueStore) String() string {
	return string(es)
}

func (es errorValueStore) Error() string {
	return string(es)
}

func (es errorValueStore) Startup() error {
	return es
}

func (es errorValueStore) Shutdown() error {
	return es
}

func (es errorValueStore) EnableWrites() error {
	return es
}

func (es errorValueStore) DisableWrites() error {
	return es
}

func (es errorValueStore) Flush() error {
	return es
}

func (es errorValueStore) AuditPass() error {
	return es
}

func (es errorValueStore) Stats(debug bool) (fmt.Stringer, error) {
	return es, es
}

func (es errorValueStore) ValueCap() (uint32, error) {
	return 0, es
}

func (es errorValueStore) Lookup(keyA uint64, keyB uint64) (int64, uint32, error) {
	return 0, 0, es
}

func (es errorValueStore) Read(keyA uint64, keyB uint64, value []byte) (int64, []byte, error) {
	return 0, value, es
}

func (es errorValueStore) Write(keyA uint64, keyB uint64, timestampMicro int64, value []byte) (int64, error) {
	return 0, es
}

func (es errorValueStore) Delete(keyA uint64, keyB uint64, timestampMicro int64) (int64, error) {
	return 0, es
}
