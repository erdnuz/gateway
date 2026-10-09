# 4. A human-first config format (version 2)

**Status:** accepted (breaking; `gate config migrate` converts version 1)

## Context

Version 1 was JSON mirroring the runtime structs: nanosecond durations,
overlapping failure knobs, and every runtime field required before defaults
applied, so a minimal file was about 100 lines.

## Decision

Version 2 is YAML (or JSON), and services are described the way people
think about them: `match`, `upstream(s)`, `limits`, `retries`. It uses human
durations, and every runtime field is optional. The file compiles to the
unchanged runtime model, so the data plane did not change with it.

Validation collects every problem in one pass, with line numbers and "did
you mean" suggestions. A JSON Schema generated from the Go types gives
editors completion, and a golden test keeps it current.

## Consequences

- The examples are 20–60 lines and read as documentation.
- The runtime model can evolve independently: the compiler is the only
  place that maps one to the other.
