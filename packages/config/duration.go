package config

import (
	"fmt"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration is a time.Duration written as a Go duration string ("90s", "5m",
// "1h30m") in config files.
type Duration time.Duration

// D returns the value as a time.Duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

// UnmarshalYAML accepts duration strings only: a bare number is ambiguous
// (seconds? nanoseconds?), so it is rejected with a hint. Errors are
// *yaml.TypeError so decoding continues and every problem is reported.
func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	fail := func(format string, args ...any) error {
		return &yaml.TypeError{Errors: []string{fmt.Sprintf("line %d: ", n.Line) + fmt.Sprintf(format, args...)}}
	}
	if n.Kind != yaml.ScalarNode {
		return fail("expected a duration such as \"30s\"")
	}
	if n.Tag == "!!int" || n.Tag == "!!float" {
		return fail("duration %s has no unit; write e.g. \"%ss\" or \"%sms\"", n.Value, n.Value, n.Value)
	}
	v, err := time.ParseDuration(strings.TrimSpace(n.Value))
	if err != nil {
		return fail("invalid duration %q (examples: 500ms, 30s, 5m, 1h)", n.Value)
	}
	if v < 0 {
		return fail("duration %q must not be negative", n.Value)
	}
	*d = Duration(v)
	return nil
}

// MarshalYAML writes the shortest duration string ("1m", not "1m0s").
func (d Duration) MarshalYAML() (any, error) { return d.String(), nil }

func (d Duration) String() string {
	s := time.Duration(d).String()
	if strings.HasSuffix(s, "m0s") {
		s = strings.TrimSuffix(s, "0s")
	}
	if strings.HasSuffix(s, "h0m") {
		s = strings.TrimSuffix(s, "0m")
	}
	return s
}

// IsZero lets omitempty drop unset durations.
func (d Duration) IsZero() bool { return d == 0 }
