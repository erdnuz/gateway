package config

import (
	"fmt"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Rate is a refill rate written as "<amount>/<unit>" ("20/s", "1200/m",
// "5000/h") in config files. It holds the rate per second.
type Rate float64

var rateUnits = map[string]float64{"s": 1, "m": 60, "h": 3600}

// PerSecond returns the rate per second.
func (r Rate) PerSecond() float64 { return float64(r) }

// UnmarshalYAML accepts "<amount>/<unit>" only: a bare number is ambiguous.
func (r *Rate) UnmarshalYAML(n *yaml.Node) error {
	fail := func(format string, args ...any) error {
		return &yaml.TypeError{Errors: []string{fmt.Sprintf("line %d: ", n.Line) + fmt.Sprintf(format, args...)}}
	}
	if n.Kind != yaml.ScalarNode {
		return fail("expected a rate such as \"20/s\"")
	}
	amount, unit, ok := strings.Cut(strings.TrimSpace(n.Value), "/")
	per, known := rateUnits[strings.TrimSpace(unit)]
	if !ok || !known {
		return fail("rate %q needs a unit; write e.g. \"%s/s\" or \"%s/m\"", n.Value, n.Value, n.Value)
	}
	v, err := strconv.ParseFloat(strings.TrimSpace(amount), 64)
	if err != nil || v <= 0 {
		return fail("invalid rate %q (examples: 20/s, 1200/m, 5000/h)", n.Value)
	}
	*r = Rate(v / per)
	return nil
}

// MarshalYAML writes the rate in the largest unit that keeps it whole.
func (r Rate) MarshalYAML() (any, error) { return r.String(), nil }

func (r Rate) String() string {
	for _, u := range []struct {
		name string
		per  float64
	}{{"s", 1}, {"m", 60}, {"h", 3600}} {
		if v := float64(r) * u.per; v == float64(int64(v)) {
			return strconv.FormatInt(int64(v), 10) + "/" + u.name
		}
	}
	return strconv.FormatFloat(float64(r), 'g', -1, 64) + "/s"
}

// IsZero lets omitempty drop unset rates.
func (r Rate) IsZero() bool { return r == 0 }
