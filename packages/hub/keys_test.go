package hub

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"gateway/packages/keys"

	"gopkg.in/yaml.v3"
)

func call(t *testing.T, hs *HubServer, method, path, token, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	hs.ServeHTTP(rec, req)
	return rec
}

type issued struct {
	Key    KeyRecord `json:"key"`
	Secret string    `json:"secret"`
}

func TestKeys_Lifecycle(t *testing.T) {
	hs, cleanup := newTestHubServer(t)
	defer cleanup()
	ctx := context.Background()

	rec := call(t, hs, "POST", "/v2/keys", "", `{"group":"v1","tier":"free","owner":"alice"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", rec.Code, rec.Body)
	}
	var created issued
	_ = json.Unmarshal(rec.Body.Bytes(), &created)
	if !strings.HasPrefix(created.Secret, "gk_"+created.Key.ID+"_") || created.Key.Owner != "alice" {
		t.Fatalf("created %+v", created)
	}
	if strings.Contains(rec.Body.String(), keys.Hash(created.Secret)) {
		t.Fatal("the response leaks the stored hash")
	}
	if tier, _ := hs.tierManager.GetTier(ctx, "v1", keys.Hash(created.Secret)); tier != "free" {
		t.Fatalf("new key resolves to %q", tier)
	}

	rec = call(t, hs, "PATCH", "/v2/keys/"+created.Key.ID, "", `{"tier":"premium"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("update: %d %s", rec.Code, rec.Body)
	}
	if tier, _ := hs.tierManager.GetTier(ctx, "v1", keys.Hash(created.Secret)); tier != "premium" {
		t.Fatalf("after update the key resolves to %q", tier)
	}

	rec = call(t, hs, "POST", "/v2/keys/"+created.Key.ID+"/rotate", "", "")
	var rotated issued
	_ = json.Unmarshal(rec.Body.Bytes(), &rotated)
	if rec.Code != http.StatusOK || rotated.Secret == created.Secret || rotated.Key.ID != created.Key.ID {
		t.Fatalf("rotate: %d %+v", rec.Code, rotated)
	}
	if tier, _ := hs.tierManager.GetTier(ctx, "v1", keys.Hash(created.Secret)); tier != "" {
		t.Fatal("the old secret still resolves after rotation")
	}
	if tier, _ := hs.tierManager.GetTier(ctx, "v1", keys.Hash(rotated.Secret)); tier != "premium" {
		t.Fatalf("rotated secret resolves to %q", tier)
	}

	if rec = call(t, hs, "GET", "/v2/keys", "", ""); !strings.Contains(rec.Body.String(), created.Key.ID) {
		t.Fatalf("list: %s", rec.Body)
	}
	if rec = call(t, hs, "DELETE", "/v2/keys/"+created.Key.ID, "", ""); rec.Code != http.StatusNoContent {
		t.Fatalf("delete: %d", rec.Code)
	}
	if tier, _ := hs.tierManager.GetTier(ctx, "v1", keys.Hash(rotated.Secret)); tier != "" {
		t.Fatal("a revoked key still resolves")
	}
	if rec = call(t, hs, "GET", "/v2/keys/"+created.Key.ID, "", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("get after delete: %d", rec.Code)
	}

	rec = call(t, hs, "GET", "/v2/audit", "", "")
	for _, action := range []string{"key.create", "key.update", "key.rotate", "key.revoke"} {
		if !strings.Contains(rec.Body.String(), action) {
			t.Errorf("audit log is missing %s: %s", action, rec.Body)
		}
	}
}

func TestKeys_RejectUnknownGroupOrTier(t *testing.T) {
	hs, cleanup := newTestHubServer(t)
	defer cleanup()
	for _, body := range []string{`{"group":"nope","tier":"free"}`, `{"group":"v1","tier":"gold"}`} {
		if rec := call(t, hs, "POST", "/v2/keys", "", body); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: %d", body, rec.Code)
		}
	}
}

func TestTokens_ReadOnlyScope(t *testing.T) {
	hs, cleanup := newTestHubServer(t)
	defer cleanup()
	hs.authToken = "admin-secret"
	hs.SetReadToken("read-secret")

	if rec := call(t, hs, "GET", "/v2/keys", "read-secret", ""); rec.Code != http.StatusOK {
		t.Fatalf("read token cannot read: %d", rec.Code)
	}
	if rec := call(t, hs, "POST", "/v2/keys", "read-secret", `{"group":"v1","tier":"free"}`); rec.Code != http.StatusForbidden {
		t.Fatalf("read token wrote: %d", rec.Code)
	}
	if rec := call(t, hs, "POST", "/v2/keys", "admin-secret", `{"group":"v1","tier":"free"}`); rec.Code != http.StatusCreated {
		t.Fatalf("admin token cannot write: %d", rec.Code)
	}
	if rec := call(t, hs, "GET", "/v2/keys", "wrong", ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("wrong token: %d", rec.Code)
	}
	if rec := call(t, hs, "GET", "/v2/audit", "admin-secret", ""); !strings.Contains(rec.Body.String(), `"actor":"admin"`) {
		t.Fatalf("audit actor: %s", rec.Body)
	}
}

// Every /v2 route is documented in the OpenAPI spec, and nothing more.
func TestOpenAPI_DocumentsEveryRoute(t *testing.T) {
	raw, err := os.ReadFile("../../api/openapi/hub.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var spec struct {
		Paths map[string]map[string]any `yaml:"paths"`
	}
	if err := yaml.Unmarshal(raw, &spec); err != nil {
		t.Fatal(err)
	}
	documented := map[string]bool{}
	for path, ops := range spec.Paths {
		if !strings.HasPrefix(path, "/v2/") {
			continue
		}
		for method := range ops {
			if method != "parameters" {
				documented[strings.ToUpper(method)+" "+path] = true
			}
		}
	}
	hs, cleanup := newTestHubServer(t)
	defer cleanup()
	for route := range hs.v2Routes() {
		if !documented[route] {
			t.Errorf("%s is not in api/openapi/hub.yaml", route)
		}
		delete(documented, route)
	}
	for route := range documented {
		t.Errorf("api/openapi/hub.yaml documents %s, which the hub does not serve", route)
	}
}
