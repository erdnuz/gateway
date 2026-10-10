package config

import (
	"encoding/json"
	"reflect"
	"strings"
)

// JSONSchema returns a JSON Schema (draft 2020-12) for config files, built
// from the File type and its desc/enum struct tags.
func JSONSchema() ([]byte, error) {
	s := schemaFor(reflect.TypeOf(File{}))
	s["$schema"] = "https://json-schema.org/draft/2020-12/schema"
	s["$id"] = "https://raw.githubusercontent.com/erdnuz/gateway/master/schema/gate.schema.json"
	s["title"] = "Gate gateway configuration (version 2)"
	return json.MarshalIndent(s, "", "  ")
}

func schemaFor(t reflect.Type) map[string]any {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == rateType {
		return map[string]any{
			"type":     "string",
			"pattern":  `^[0-9]+(\.[0-9]+)?/(s|m|h)$`,
			"examples": []string{"20/s", "1200/m", "5000/h"},
		}
	}
	if t == durationType {
		return map[string]any{
			"type":     "string",
			"pattern":  `^([0-9]+(\.[0-9]+)?(ns|us|µs|ms|s|m|h))+$`,
			"examples": []string{"500ms", "30s", "5m", "1h"},
		}
	}
	switch t.Kind() {
	case reflect.Struct:
		props := map[string]any{}
		var required []string
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			parts := strings.Split(f.Tag.Get("yaml"), ",")
			name := parts[0]
			if name == "" || name == "-" {
				continue
			}
			p := schemaFor(f.Type)
			if d := f.Tag.Get("desc"); d != "" {
				p["description"] = d
			}
			if e := f.Tag.Get("enum"); e != "" {
				p["enum"] = strings.Split(e, ",")
			}
			props[name] = p
			if !contains(parts[1:], "omitempty") && f.Type.Kind() != reflect.Pointer {
				required = append(required, name)
			}
		}
		out := map[string]any{"type": "object", "properties": props, "additionalProperties": false}
		if len(required) > 0 {
			out["required"] = required
		}
		return out
	case reflect.Map:
		return map[string]any{"type": "object", "additionalProperties": schemaFor(t.Elem())}
	case reflect.Slice:
		return map[string]any{"type": "array", "items": schemaFor(t.Elem())}
	case reflect.String:
		return map[string]any{"type": "string"}
	case reflect.Bool:
		return map[string]any{"type": "boolean"}
	case reflect.Float32, reflect.Float64:
		return map[string]any{"type": "number"}
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint:
		return map[string]any{"type": "integer", "minimum": 0}
	default:
		return map[string]any{"type": "integer"}
	}
}

func contains(xs []string, v string) bool {
	for _, x := range xs {
		if x == v {
			return true
		}
	}
	return false
}
