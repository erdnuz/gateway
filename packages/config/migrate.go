package config

import (
	"encoding/json"
	"fmt"

	"gateway/packages/common/types"
)

// MigrateV1 converts a version 1 (JSON, nanosecond durations) config into a
// version 2 File. The result is validated; problems such as service IDs that
// are not valid v2 names are reported so they can be fixed by hand.
func MigrateV1(data []byte) (*File, error) {
	var v1 types.GatewayConfig
	if err := json.Unmarshal(data, &v1); err != nil {
		return nil, fmt.Errorf("not a version 1 config: %w", err)
	}
	if len(v1.Prefixes) == 0 {
		return nil, fmt.Errorf("not a version 1 config: no prefixes")
	}
	f := FromInternal(&v1)
	out, err := Marshal(f)
	if err != nil {
		return nil, err
	}
	if _, err := Parse(out, "migrated config"); err != nil {
		return f, fmt.Errorf("migrated config needs manual fixes:\n%w", err)
	}
	return f, nil
}
