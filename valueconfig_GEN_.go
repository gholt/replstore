package replstore

// ReplValueStoreConfig defines the settings when calling NewValueStore.
type ReplValueStoreConfig struct {
	// AddressIndex indicates which of the ring node addresses to use when
	// connecting to a node (see github.com/gholt/ring/Node.Address).
	AddressIndex int
}

func resolveReplValueStoreConfig(c *ReplValueStoreConfig) *ReplValueStoreConfig {
	cfg := &ReplValueStoreConfig{}
	if c != nil {
		*cfg = *c
	}
	// AddressIndex default of 0 is fine.
	return cfg
}
