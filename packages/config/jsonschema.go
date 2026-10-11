package config

import (
	"encoding/json"
	"reflect"
	"strings"
)

// JSONSchema returns a JSON Schema (draft 2020-12) for config files, built
// from the File type and its desc/enum struct tags.
func JSONSchema() ([]byte, error) {
	g := &schemaGen{defs: map[string]any{}, visiting: map[reflect.Type]bool{}, recursive: map[reflect.Type]bool{}}
	s := g.schemaFor(reflect.TypeOf(File{}))
	if len(g.defs) > 0 {
		s["$defs"] = g.defs
	}
	s["$schema"] = "https://json-schema.org/draft/2020-12/schema"
	s["$id"] = "https://raw.githubusercontent.com/erdnuz/gateway/master/schema/gate.schema.json"
	s["title"] = "Gate gateway configuration (version 2)"
	return json.MarshalIndent(s, "", "  ")
}

// schemaGen builds schemas, emitting recursive types (descriptor entries
// nest) once under $defs and referring to them.
type schemaGen struct {
	defs      map[string]any
	visiting  map[reflect.Type]bool
	recursive map[reflect.Type]bool
}

func (g *schemaGen) schemaFor(t reflect.Type) map[string]any {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() == reflect.Struct && t != durationType && t != rateType {
		ref := map[string]any{"$ref": "#/$defs/" + t.Name()}
		if g.visiting[t] {
			g.recursive[t] = true
			return ref
		}
		if _, done := g.defs[t.Name()]; done {
			return ref
		}
		g.visiting[t] = true
		s := g.structSchema(t)
		delete(g.visiting, t)
		if g.recursive[t] {
			g.defs[t.Name()] = s
			return ref
		}
		return s
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
	case reflect.Map:
		return map[string]any{"type": "object", "additionalProperties": g.schemaFor(t.Elem())}
	case reflect.Slice:
		return map[string]any{"type": "array", "items": g.schemaFor(t.Elem())}
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

func (g *schemaGen) structSchema(t reflect.Type) map[string]any {
	props := map[string]any{}
	var required []string
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		parts := strings.Split(f.Tag.Get("yaml"), ",")
		name := parts[0]
		if name == "" || name == "-" {
			continue
		}
		p := g.schemaFor(f.Type)
		if d := f.Tag.Get("desc"); d != "" {
			p = withKeyword(p, "description", d)
		}
		if e := f.Tag.Get("enum"); e != "" {
			p = withKeyword(p, "enum", strings.Split(e, ","))
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
}

// withKeyword adds a keyword to a schema, copying a shared $ref schema
// rather than changing it in place.
func withKeyword(s map[string]any, k string, v any) map[string]any {
	if _, ref := s["$ref"]; ref {
		c := map[string]any{"$ref": s["$ref"]}
		c[k] = v
		return c
	}
	s[k] = v
	return s
}

func contains(xs []string, v string) bool {
	for _, x := range xs {
		if x == v {
			return true
		}
	}
	return false
}
