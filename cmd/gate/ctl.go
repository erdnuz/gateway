package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"text/tabwriter"
	"time"
)

const ctlUsage = `usage: gate ctl <command>   (hub: $GATE_HUB, token: $GATE_TOKEN)

  keys create -group G -tier T [-owner O] [-name N]   issue a key (the secret is shown once)
  keys list [-group G]                                list issued keys
  keys set-tier <id> <tier>                           change a key's tier on every edge
  keys rotate <id>                                    replace a key's secret
  keys revoke <id>                                    revoke a key everywhere
  status                                              hub readiness and connected edges
  audit [-n 20]                                       recent administrative changes
  config diff <file>                                  what reloading <file> would change
  config apply <file>                                 validate and roll out <file> to every edge`

// ctl talks to the hub's admin API.
type ctl struct {
	hub, token string
	client     *http.Client
	out        io.Writer
}

func runCtl(args []string) error {
	c := &ctl{
		hub:    strings.TrimRight(envOr("GATE_HUB", "http://localhost:8081"), "/"),
		token:  envOr("GATE_TOKEN", os.Getenv("HUB_AUTH_TOKEN")),
		client: &http.Client{Timeout: 10 * time.Second},
		out:    os.Stdout,
	}
	if len(args) == 0 {
		return errors.New(ctlUsage)
	}
	switch cmd, rest := args[0], args[1:]; cmd {
	case "keys":
		return c.keys(rest)
	case "status":
		var st map[string]any
		if err := c.do("GET", "/status", nil, &st); err != nil {
			return err
		}
		fmt.Fprintf(c.out, "hub %s  ready=%v  edges connected=%v\n", c.hub, st["ready"], st["edges_connected"])
		return nil
	case "audit":
		fs := flag.NewFlagSet("audit", flag.ContinueOnError)
		n := fs.Int("n", 20, "entries to show")
		if err := fs.Parse(rest); err != nil {
			return ignoreHelp(err)
		}
		var res struct {
			Entries []map[string]string `json:"entries"`
		}
		if err := c.do("GET", fmt.Sprintf("/v2/audit?count=%d", *n), nil, &res); err != nil {
			return err
		}
		tw := tabwriter.NewWriter(c.out, 0, 2, 2, ' ', 0)
		fmt.Fprintln(tw, "TIME\tACTOR\tACTION\tTARGET\tDETAIL")
		for _, e := range res.Entries {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", e["time"], e["actor"], e["action"], e["target"], e["detail"])
		}
		return tw.Flush()
	case "config":
		if len(rest) != 2 || (rest[0] != "diff" && rest[0] != "apply") {
			return errors.New(ctlUsage)
		}
		body, err := os.ReadFile(rest[1])
		if err != nil {
			return err
		}
		if rest[0] == "apply" {
			if err := c.do("POST", "/config-reload", body, nil); err != nil {
				return err
			}
			fmt.Fprintf(c.out, "applied %s; edges receive it over their watch streams\n", rest[1])
			return nil
		}
		var report map[string]any
		if err := c.do("POST", "/config-reload/dry-run", body, &report); err != nil {
			return err
		}
		b, _ := json.MarshalIndent(report["analysis"], "", "  ")
		fmt.Fprintln(c.out, string(b))
		return nil
	case "help", "-h", "--help":
		fmt.Fprintln(c.out, ctlUsage)
		return nil
	}
	return errors.New(ctlUsage)
}

type keyView struct {
	ID        string    `json:"id"`
	Group     string    `json:"group"`
	Tier      string    `json:"tier"`
	Owner     string    `json:"owner"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

func (c *ctl) keys(args []string) error {
	if len(args) == 0 {
		return errors.New(ctlUsage)
	}
	switch cmd, rest := args[0], args[1:]; cmd {
	case "create":
		fs := flag.NewFlagSet("keys create", flag.ContinueOnError)
		group := fs.String("group", "", "route group (first path segment), e.g. v1")
		tier := fs.String("tier", "", "tier name")
		owner := fs.String("owner", "", "who the key is for")
		name := fs.String("name", "", "label")
		if err := fs.Parse(rest); err != nil {
			return ignoreHelp(err)
		}
		body, _ := json.Marshal(map[string]string{"group": *group, "tier": *tier, "owner": *owner, "name": *name})
		var res struct {
			Key    keyView `json:"key"`
			Secret string  `json:"secret"`
		}
		if err := c.do("POST", "/v2/keys", body, &res); err != nil {
			return err
		}
		fmt.Fprintf(c.out, "key %s (group %s, tier %s)\n\n  %s\n\nStore it now: the secret cannot be shown again.\n", res.Key.ID, res.Key.Group, res.Key.Tier, res.Secret)
		return nil
	case "list":
		fs := flag.NewFlagSet("keys list", flag.ContinueOnError)
		group := fs.String("group", "", "only keys of this group")
		if err := fs.Parse(rest); err != nil {
			return ignoreHelp(err)
		}
		var res struct {
			Keys []keyView `json:"keys"`
		}
		if err := c.do("GET", "/v2/keys?group="+*group, nil, &res); err != nil {
			return err
		}
		tw := tabwriter.NewWriter(c.out, 0, 2, 2, ' ', 0)
		fmt.Fprintln(tw, "ID\tGROUP\tTIER\tOWNER\tNAME\tCREATED")
		for _, k := range res.Keys {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n", k.ID, k.Group, k.Tier, k.Owner, k.Name, k.CreatedAt.Format(time.RFC3339))
		}
		return tw.Flush()
	case "set-tier":
		if len(rest) != 2 {
			return errors.New(ctlUsage)
		}
		body, _ := json.Marshal(map[string]string{"tier": rest[1]})
		if err := c.do("PATCH", "/v2/keys/"+rest[0], body, nil); err != nil {
			return err
		}
		fmt.Fprintf(c.out, "key %s is now on tier %s\n", rest[0], rest[1])
		return nil
	case "rotate":
		if len(rest) != 1 {
			return errors.New(ctlUsage)
		}
		var res struct {
			Secret string `json:"secret"`
		}
		if err := c.do("POST", "/v2/keys/"+rest[0]+"/rotate", nil, &res); err != nil {
			return err
		}
		fmt.Fprintf(c.out, "new secret for %s (the old one no longer works):\n\n  %s\n", rest[0], res.Secret)
		return nil
	case "revoke":
		if len(rest) != 1 {
			return errors.New(ctlUsage)
		}
		if err := c.do("DELETE", "/v2/keys/"+rest[0], nil, nil); err != nil {
			return err
		}
		fmt.Fprintf(c.out, "key %s revoked\n", rest[0])
		return nil
	}
	return errors.New(ctlUsage)
}

// do sends a request and decodes a JSON response into out (when non-nil).
// Problem responses become errors carrying their detail.
func (c *ctl) do(method, path string, body []byte, out any) error {
	req, err := http.NewRequest(method, c.hub+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("hub %s: %w", c.hub, err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if resp.StatusCode/100 != 2 {
		var p struct{ Title, Detail, Code string }
		if json.Unmarshal(data, &p) == nil && p.Code != "" {
			return fmt.Errorf("%s (%s): %s", p.Title, p.Code, p.Detail)
		}
		return fmt.Errorf("hub returned %s: %s", resp.Status, strings.TrimSpace(string(data)))
	}
	if out != nil && len(data) > 0 {
		return json.Unmarshal(data, out)
	}
	return nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
