package config

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"gateway/packages/common/types"

	"gopkg.in/yaml.v3"
)

// Problem is one thing wrong with a config file.
type Problem struct {
	Line int    // 1-based; 0 when unknown
	Path string // e.g. services[0].limits.tiers.pro.requests
	Msg  string
}

func (p Problem) String() string {
	var b strings.Builder
	if p.Line > 0 {
		fmt.Fprintf(&b, "line %d: ", p.Line)
	}
	if p.Path != "" {
		b.WriteString(p.Path + ": ")
	}
	b.WriteString(p.Msg)
	return b.String()
}

// Error lists every problem found in a config file.
type Error struct {
	File     string
	Problems []Problem
}

func (e *Error) Error() string {
	var b strings.Builder
	name := e.File
	if name == "" {
		name = "config"
	}
	fmt.Fprintf(&b, "%s: %d problem(s)", name, len(e.Problems))
	for _, p := range e.Problems {
		b.WriteString("\n  " + p.String())
	}
	return b.String()
}

// Load reads, validates and compiles a config file.
func Load(path string) (*types.GatewayConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return ParseAndCompile(data, path)
}

// ParseAndCompile validates a config document and compiles it to the runtime
// model served to edges.
func ParseAndCompile(data []byte, name string) (*types.GatewayConfig, error) {
	f, err := Parse(data, name)
	if err != nil {
		return nil, err
	}
	return Compile(f), nil
}

// Parse decodes and validates a YAML or JSON config document. On failure it
// returns an *Error describing every problem it found.
func Parse(data []byte, name string) (*File, error) {
	p := &parser{file: name, nodes: map[string]*yaml.Node{}}
	var root yaml.Node
	if err := yaml.Unmarshal(data, &root); err != nil {
		p.add(lineOf(err.Error()), "", strings.TrimPrefix(stripLine(err.Error()), "yaml: "))
		return nil, p.err()
	}
	if len(root.Content) == 0 {
		p.add(0, "", "file is empty")
		return nil, p.err()
	}
	doc := root.Content[0]
	if doc.Kind != yaml.MappingNode {
		p.add(doc.Line, "", "expected a mapping with version and services")
		return nil, p.err()
	}
	if hasKey(doc, "prefixes") && !hasKey(doc, "version") {
		p.add(doc.Line, "", "this is a version 1 config; convert it with: gate config migrate "+displayName(name))
		return nil, p.err()
	}

	p.walk(doc, reflect.TypeOf(File{}), "")
	var f File
	if err := doc.Decode(&f); err != nil {
		var te *yaml.TypeError
		if errors.As(err, &te) {
			for _, msg := range te.Errors {
				p.add(lineOf(msg), "", friendlyTypeError(stripLine(msg)))
			}
		} else {
			p.add(0, "", err.Error())
		}
	}
	// Semantic checks run even after structural problems so one pass reports
	// everything, except where a line is already flagged (a misspelled or
	// mistyped field would otherwise also be reported as missing).
	structural := map[int]bool{}
	for _, pr := range p.problems {
		structural[pr.Line] = true
	}
	n := len(p.problems)
	p.validate(&f)
	kept := p.problems[:n]
	for _, pr := range p.problems[n:] {
		if pr.Line == 0 || !structural[pr.Line] {
			kept = append(kept, pr)
		}
	}
	p.problems = kept
	if len(p.problems) > 0 {
		return nil, p.err()
	}
	return &f, nil
}

type parser struct {
	file     string
	nodes    map[string]*yaml.Node // value node by path, for line numbers
	problems []Problem
}

func (p *parser) add(line int, path, msg string) {
	p.problems = append(p.problems, Problem{Line: line, Path: path, Msg: msg})
}

// at reports a problem located at the node for path (or its nearest parent).
func (p *parser) at(path, format string, args ...any) {
	line := 0
	for q := path; ; {
		if n, ok := p.nodes[q]; ok {
			line = n.Line
			break
		}
		i := strings.LastIndexAny(q, ".[")
		if i < 0 {
			break
		}
		q = q[:i]
	}
	p.add(line, path, fmt.Sprintf(format, args...))
}

func (p *parser) err() error {
	sort.SliceStable(p.problems, func(i, j int) bool { return p.problems[i].Line < p.problems[j].Line })
	return &Error{File: p.file, Problems: p.problems}
}

var durationType = reflect.TypeOf(Duration(0))

// walk records node positions and reports keys the schema does not define.
func (p *parser) walk(n *yaml.Node, t reflect.Type, path string) {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if path != "" {
		p.nodes[path] = n
	}
	if n.Kind == yaml.AliasNode {
		n = n.Alias
	}
	if t == durationType {
		return
	}
	switch t.Kind() {
	case reflect.Struct:
		if n.Kind != yaml.MappingNode {
			return // Decode reports the type mismatch
		}
		fields := yamlFields(t)
		for i := 0; i+1 < len(n.Content); i += 2 {
			key, val := n.Content[i], n.Content[i+1]
			if key.Tag == "!!merge" { // `<<: *anchor` merges another mapping in
				merged := []*yaml.Node{val}
				if val.Kind == yaml.SequenceNode {
					merged = val.Content
				}
				for _, m := range merged {
					p.walk(m, t, path)
				}
				continue
			}
			child := joinPath(path, key.Value)
			f, ok := fields[key.Value]
			if !ok {
				msg := fmt.Sprintf("unknown field %q", key.Value)
				if s := suggest(key.Value, fieldNames(fields)); s != "" {
					msg += fmt.Sprintf(" (did you mean %q?)", s)
				}
				p.add(key.Line, child, msg)
				continue
			}
			p.walk(val, f.Type, child)
		}
	case reflect.Map:
		if n.Kind != yaml.MappingNode {
			return
		}
		for i := 0; i+1 < len(n.Content); i += 2 {
			p.walk(n.Content[i+1], t.Elem(), joinPath(path, n.Content[i].Value))
		}
	case reflect.Slice:
		if n.Kind != yaml.SequenceNode {
			return
		}
		for i, el := range n.Content {
			p.walk(el, t.Elem(), fmt.Sprintf("%s[%d]", path, i))
		}
	}
}

func yamlFields(t reflect.Type) map[string]reflect.StructField {
	out := map[string]reflect.StructField{}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		name := strings.Split(f.Tag.Get("yaml"), ",")[0]
		if name != "" && name != "-" {
			out[name] = f
		}
	}
	return out
}

func fieldNames(fields map[string]reflect.StructField) []string {
	names := make([]string, 0, len(fields))
	for n := range fields {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

func joinPath(path, key string) string {
	if path == "" {
		return key
	}
	return path + "." + key
}

func hasKey(n *yaml.Node, key string) bool {
	for i := 0; i+1 < len(n.Content); i += 2 {
		if n.Content[i].Value == key {
			return true
		}
	}
	return false
}

var lineRe = regexp.MustCompile(`line (\d+)`)

func lineOf(msg string) int {
	if m := lineRe.FindStringSubmatch(msg); m != nil {
		n, _ := strconv.Atoi(m[1])
		return n
	}
	return 0
}

func stripLine(msg string) string {
	msg = strings.TrimPrefix(msg, "yaml: ")
	if i := strings.Index(msg, ": "); i >= 0 && strings.HasPrefix(msg, "line ") {
		return msg[i+2:]
	}
	return msg
}

// friendlyTypeError rewrites yaml.v3's Go-centric messages.
func friendlyTypeError(msg string) string {
	r := strings.NewReplacer(
		"cannot unmarshal !!str", "expected a different type, got string",
		"cannot unmarshal !!int", "expected a different type, got number",
		"cannot unmarshal !!seq", "expected a different type, got a list",
		"cannot unmarshal !!map", "expected a different type, got a mapping",
		"config.", "",
	)
	return r.Replace(msg)
}

// suggest returns the closest candidate within edit distance 2, or "".
func suggest(word string, candidates []string) string {
	best, bestD := "", 3
	for _, c := range candidates {
		if d := editDistance(strings.ToLower(word), c); d < bestD {
			best, bestD = c, d
		}
	}
	return best
}

func editDistance(a, b string) int {
	prev := make([]int, len(b)+1)
	cur := make([]int, len(b)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		cur[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			cur[j] = min(prev[j]+1, cur[j-1]+1, prev[j-1]+cost)
		}
		prev, cur = cur, prev
	}
	return prev[len(b)]
}

func displayName(name string) string {
	if name == "" {
		return "<file>"
	}
	return name
}

// Marshal writes a File as YAML with a schema reference for editors.
func Marshal(f *File) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteString("# yaml-language-server: $schema=https://raw.githubusercontent.com/erdnuz/gateway/master/schema/gate.schema.json\n")
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(f); err != nil {
		return nil, err
	}
	return buf.Bytes(), enc.Close()
}
