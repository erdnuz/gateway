package hub

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"gateway/packages/common/httpx"
	"gateway/packages/keys"

	"github.com/redis/go-redis/v9"
)

// KeyRecord describes an issued API key. The secret itself is never stored:
// only its hash, which is also all the edges and the quota ledger ever see.
type KeyRecord struct {
	ID        string     `json:"id"`
	Group     string     `json:"group"`
	Tier      string     `json:"tier"`
	Owner     string     `json:"owner,omitempty"`
	Name      string     `json:"name,omitempty"`
	CreatedAt time.Time  `json:"created_at"`
	RotatedAt *time.Time `json:"rotated_at,omitempty"`
	Hash      string     `json:"-"`
}

// keyRecordJSON is the stored form, which keeps the hash.
type keyRecordJSON struct {
	KeyRecord
	Hash string `json:"hash"`
}

// Key records share one Redis Cluster hash tag, so creating or deleting a
// key (record plus index) is one transaction on one shard.
const (
	keyIndex    = "hub:{keys}"
	keyRecordNS = "hub:{keys}:"
	// The layout before hash tags, migrated by MigrateKeyLayout.
	legacyKeyIndex    = "hub:keys"
	legacyKeyRecordNS = "hub:key:"
	auditStream       = "hub:audit"
	auditMaxLen       = 10000
)

var errNoKey = errors.New("no such key")

// MigrateKeyLayout moves key records written before the Cluster-ready
// layout to it. It is idempotent and a no-op on a fresh Redis.
func MigrateKeyLayout(ctx context.Context, rdb redis.UniversalClient) error {
	ids, err := rdb.SMembers(ctx, legacyKeyIndex).Result()
	if err != nil || len(ids) == 0 {
		return err
	}
	for _, id := range ids {
		b, err := rdb.Get(ctx, legacyKeyRecordNS+id).Bytes()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			return err
		}
		if _, err := rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
			p.Set(ctx, keyRecordNS+id, b, 0)
			p.SAdd(ctx, keyIndex, id)
			return nil
		}); err != nil {
			return err
		}
		if err := rdb.Del(ctx, legacyKeyRecordNS+id).Err(); err != nil {
			return err
		}
	}
	return rdb.Del(ctx, legacyKeyIndex).Err()
}

func (s *HubServer) loadKey(ctx context.Context, id string) (*KeyRecord, error) {
	b, err := s.rdb.Get(ctx, keyRecordNS+id).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, errNoKey
	}
	if err != nil {
		return nil, err
	}
	var rec keyRecordJSON
	if err := json.Unmarshal(b, &rec); err != nil {
		return nil, err
	}
	rec.KeyRecord.Hash = rec.Hash
	return &rec.KeyRecord, nil
}

func (s *HubServer) saveKey(ctx context.Context, rec *KeyRecord) error {
	b, err := json.Marshal(keyRecordJSON{KeyRecord: *rec, Hash: rec.Hash})
	if err != nil {
		return err
	}
	_, err = s.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.Set(ctx, keyRecordNS+rec.ID, b, 0)
		p.SAdd(ctx, keyIndex, rec.ID)
		return nil
	})
	return err
}

// assign makes hash resolve to tier on every edge (tier "" revokes it) and
// records its owner (nil leaves it), whose keys share any org budget.
func (s *HubServer) assign(ctx context.Context, group, hash, tier string, owner *string) error {
	var err error
	if tier == "" {
		owner = new(string)
		err = s.tierManager.DeleteTier(ctx, group, hash)
	} else {
		err = s.tierManager.SetTier(ctx, group, hash, tier)
	}
	if owners, ok := s.tierManager.(OwnerStore); ok && err == nil && owner != nil {
		err = owners.SetOwner(ctx, group, hash, *owner)
	}
	if err == nil {
		s.publishTier(group, hash, tier)
	}
	return err
}

// v2Routes is the /v2 admin API; api/openapi/hub.yaml documents each route
// (a test keeps them in step).
func (s *HubServer) v2Routes() map[string]http.HandlerFunc {
	return map[string]http.HandlerFunc{
		"POST /v2/keys":             s.createKey,
		"GET /v2/keys":              s.listKeys,
		"GET /v2/keys/{id}":         s.getKey,
		"PATCH /v2/keys/{id}":       s.updateKey,
		"POST /v2/keys/{id}/rotate": s.rotateKey,
		"DELETE /v2/keys/{id}":      s.deleteKey,
		"GET /v2/audit":             s.listAudit,
	}
}

func (s *HubServer) keyRoutes() *http.ServeMux {
	mux := http.NewServeMux()
	for pattern, h := range s.v2Routes() {
		mux.HandleFunc(pattern, h)
	}
	return mux
}

type keyRequest struct {
	Group *string `json:"group"`
	Tier  *string `json:"tier"`
	Owner *string `json:"owner"`
	Name  *string `json:"name"`
}

func decodeKeyRequest(w http.ResponseWriter, r *http.Request) (keyRequest, bool) {
	var req keyRequest
	if r.ContentLength == 0 {
		return req, true
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 8<<10)).Decode(&req); err != nil {
		httpx.WriteProblem(w, r, http.StatusBadRequest, "bad_request", "body must be JSON: "+err.Error())
		return req, false
	}
	return req, true
}

func deref(p *string) string {
	if p == nil {
		return ""
	}
	return strings.TrimSpace(*p)
}

// createKey issues a key. The response is the only time the secret exists
// outside the caller's hands.
func (s *HubServer) createKey(w http.ResponseWriter, r *http.Request) {
	req, ok := decodeKeyRequest(w, r)
	if !ok {
		return
	}
	group, tier := deref(req.Group), deref(req.Tier)
	if !s.isAllowedPrefix(group) {
		httpx.WriteProblem(w, r, http.StatusBadRequest, "unknown_group", "group must be the first path segment of a configured service, e.g. v1")
		return
	}
	if !s.tierDefinedInPrefix(group, tier) {
		httpx.WriteProblem(w, r, http.StatusBadRequest, "unknown_tier", "tier is not defined by any service in the group")
		return
	}
	id, secret := keys.New()
	rec := &KeyRecord{ID: id, Group: group, Tier: tier, Owner: deref(req.Owner), Name: deref(req.Name), CreatedAt: time.Now().UTC(), Hash: keys.Hash(secret)}
	if err := s.saveKey(r.Context(), rec); err != nil {
		s.writeInternalError(w, r, "createKey.save", err)
		return
	}
	if err := s.assign(r.Context(), group, rec.Hash, tier, &rec.Owner); err != nil {
		s.writeInternalError(w, r, "createKey.assign", err)
		return
	}
	s.audit(r, "key.create", id, "group="+group+" tier="+tier)
	httpx.WriteJSON(w, http.StatusCreated, map[string]any{"key": rec, "secret": secret})
}

func (s *HubServer) listKeys(w http.ResponseWriter, r *http.Request) {
	ids, err := s.rdb.SMembers(r.Context(), keyIndex).Result()
	if err != nil {
		s.writeInternalError(w, r, "listKeys", err)
		return
	}
	group := r.URL.Query().Get("group")
	out := []*KeyRecord{}
	for _, id := range ids {
		rec, err := s.loadKey(r.Context(), id)
		if err != nil {
			continue // deleted concurrently
		}
		if group == "" || rec.Group == group {
			out = append(out, rec)
		}
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"keys": out})
}

func (s *HubServer) keyFromPath(w http.ResponseWriter, r *http.Request) (*KeyRecord, bool) {
	rec, err := s.loadKey(r.Context(), r.PathValue("id"))
	if errors.Is(err, errNoKey) {
		httpx.WriteProblem(w, r, http.StatusNotFound, "no_such_key", "")
		return nil, false
	}
	if err != nil {
		s.writeInternalError(w, r, "loadKey", err)
		return nil, false
	}
	return rec, true
}

func (s *HubServer) getKey(w http.ResponseWriter, r *http.Request) {
	if rec, ok := s.keyFromPath(w, r); ok {
		httpx.WriteJSON(w, http.StatusOK, rec)
	}
}

// updateKey changes a key's tier (pushed to every edge at once) or labels.
func (s *HubServer) updateKey(w http.ResponseWriter, r *http.Request) {
	rec, ok := s.keyFromPath(w, r)
	if !ok {
		return
	}
	req, ok := decodeKeyRequest(w, r)
	if !ok {
		return
	}
	if req.Group != nil {
		httpx.WriteProblem(w, r, http.StatusBadRequest, "immutable_group", "a key's group cannot change; issue a new key")
		return
	}
	if req.Owner != nil {
		rec.Owner = deref(req.Owner)
	}
	if req.Name != nil {
		rec.Name = deref(req.Name)
	}
	ownerChanged := req.Owner != nil
	tierChanged := req.Tier != nil && deref(req.Tier) != rec.Tier
	if tierChanged {
		if !s.tierDefinedInPrefix(rec.Group, deref(req.Tier)) {
			httpx.WriteProblem(w, r, http.StatusBadRequest, "unknown_tier", "tier is not defined by any service in the group")
			return
		}
		rec.Tier = deref(req.Tier)
	}
	if err := s.saveKey(r.Context(), rec); err != nil {
		s.writeInternalError(w, r, "updateKey.save", err)
		return
	}
	if tierChanged || ownerChanged {
		if err := s.assign(r.Context(), rec.Group, rec.Hash, rec.Tier, &rec.Owner); err != nil {
			s.writeInternalError(w, r, "updateKey.assign", err)
			return
		}
	}
	s.audit(r, "key.update", rec.ID, "tier="+rec.Tier)
	httpx.WriteJSON(w, http.StatusOK, rec)
}

// rotateKey replaces a key's secret. The old secret stops working on every
// edge as soon as the change is pushed.
func (s *HubServer) rotateKey(w http.ResponseWriter, r *http.Request) {
	rec, ok := s.keyFromPath(w, r)
	if !ok {
		return
	}
	oldHash := rec.Hash
	secret := keys.Rotate(rec.ID)
	now := time.Now().UTC()
	rec.Hash, rec.RotatedAt = keys.Hash(secret), &now
	if err := s.saveKey(r.Context(), rec); err != nil {
		s.writeInternalError(w, r, "rotateKey.save", err)
		return
	}
	if err := s.assign(r.Context(), rec.Group, rec.Hash, rec.Tier, &rec.Owner); err != nil {
		s.writeInternalError(w, r, "rotateKey.assign", err)
		return
	}
	if err := s.assign(r.Context(), rec.Group, oldHash, "", nil); err != nil {
		s.writeInternalError(w, r, "rotateKey.revoke", err)
		return
	}
	s.audit(r, "key.rotate", rec.ID, "")
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"key": rec, "secret": secret})
}

func (s *HubServer) deleteKey(w http.ResponseWriter, r *http.Request) {
	rec, ok := s.keyFromPath(w, r)
	if !ok {
		return
	}
	if err := s.assign(r.Context(), rec.Group, rec.Hash, "", nil); err != nil {
		s.writeInternalError(w, r, "deleteKey.revoke", err)
		return
	}
	if _, err := s.rdb.TxPipelined(r.Context(), func(p redis.Pipeliner) error {
		p.Del(r.Context(), keyRecordNS+rec.ID)
		p.SRem(r.Context(), keyIndex, rec.ID)
		return nil
	}); err != nil {
		s.writeInternalError(w, r, "deleteKey", err)
		return
	}
	s.audit(r, "key.revoke", rec.ID, "")
	w.WriteHeader(http.StatusNoContent)
}

// AuditEntry is one administrative change.
type AuditEntry struct {
	ID        string `json:"id"`
	Time      string `json:"time"`
	Actor     string `json:"actor"`
	Action    string `json:"action"`
	Target    string `json:"target"`
	Detail    string `json:"detail,omitempty"`
	RequestID string `json:"request_id,omitempty"`
}

// audit appends a change to a bounded Redis stream shared by every hub.
func (s *HubServer) audit(r *http.Request, action, target, detail string) {
	if s.rdb == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), time.Second)
	defer cancel()
	err := s.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: auditStream, MaxLen: auditMaxLen, Approx: true,
		Values: map[string]any{
			"time": time.Now().UTC().Format(time.RFC3339), "actor": roleOf(r.Context()),
			"action": action, "target": target, "detail": detail, "request_id": httpx.RequestID(r.Context()),
		},
	}).Err()
	if err != nil {
		httpx.Logger(r.Context()).Warn("audit write failed", "action", action, "err", err)
	}
}

func (s *HubServer) listAudit(w http.ResponseWriter, r *http.Request) {
	count, _ := strconv.Atoi(r.URL.Query().Get("count"))
	if count <= 0 || count > 1000 {
		count = 50
	}
	msgs, err := s.rdb.XRevRangeN(r.Context(), auditStream, "+", "-", int64(count)).Result()
	if err != nil {
		s.writeInternalError(w, r, "listAudit", err)
		return
	}
	out := make([]AuditEntry, 0, len(msgs))
	for _, m := range msgs {
		v := func(k string) string { s, _ := m.Values[k].(string); return s }
		out = append(out, AuditEntry{ID: m.ID, Time: v("time"), Actor: v("actor"), Action: v("action"), Target: v("target"), Detail: v("detail"), RequestID: v("request_id")})
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"entries": out})
}

type roleKey struct{}

func roleOf(ctx context.Context) string {
	role, _ := ctx.Value(roleKey{}).(string)
	return role
}
