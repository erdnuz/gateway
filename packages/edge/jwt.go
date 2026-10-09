package edge

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"sync"
	"time"

	"gateway/packages/common/types"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/sync/singleflight"
)

const (
	jwksTTL        = 10 * time.Minute
	jwksMinRefresh = 30 * time.Second // floor between fetches for unknown key IDs
)

var errUnknownKey = errors.New("token signed by an unknown key")

// jwtVerifier checks bearer tokens against each service's JWKS. Key sets are
// cached per URL, so verification is local to the edge: no call to the
// issuer per request, and one fetch per key set however many requests
// arrive together.
type jwtVerifier struct {
	client  *http.Client
	sets    sync.Map // JWKS URL -> *keySet
	flights singleflight.Group
}

type keySet struct {
	keys    map[string]any // kid -> *rsa.PublicKey or *ecdsa.PublicKey
	fetched time.Time
}

func newJWTVerifier() *jwtVerifier {
	return &jwtVerifier{client: &http.Client{Timeout: 5 * time.Second}}
}

// verify checks a token's signature, expiry, issuer and audience, and
// returns its claims.
func (v *jwtVerifier) verify(ctx context.Context, cfg *types.JWTConfig, raw string) (jwt.MapClaims, error) {
	opts := []jwt.ParserOption{
		jwt.WithValidMethods([]string{"RS256", "RS384", "RS512", "PS256", "PS384", "PS512", "ES256", "ES384", "ES512"}),
		jwt.WithExpirationRequired(),
		jwt.WithLeeway(30 * time.Second),
	}
	if cfg.Issuer != "" {
		opts = append(opts, jwt.WithIssuer(cfg.Issuer))
	}
	if cfg.Audience != "" {
		opts = append(opts, jwt.WithAudience(cfg.Audience))
	}
	claims := jwt.MapClaims{}
	_, err := jwt.ParseWithClaims(raw, claims, func(t *jwt.Token) (any, error) {
		kid, _ := t.Header["kid"].(string)
		return v.key(ctx, cfg.JWKSURL, kid)
	}, opts...)
	return claims, err
}

// key finds a public key by ID, refreshing the set when it is stale or the
// ID is new (keys rotate), but no more often than jwksMinRefresh.
func (v *jwtVerifier) key(ctx context.Context, url, kid string) (any, error) {
	var set *keySet
	if cached, ok := v.sets.Load(url); ok {
		set = cached.(*keySet)
		if k, ok := set.keys[kid]; ok && time.Since(set.fetched) < jwksTTL {
			return k, nil
		}
	}
	if set == nil || time.Since(set.fetched) > jwksMinRefresh {
		fresh, err := v.fetch(ctx, url)
		if err != nil {
			if set == nil {
				return nil, err
			}
		} else {
			set = fresh
		}
	}
	if k, ok := set.keys[kid]; ok {
		return k, nil
	}
	return nil, errUnknownKey
}

func (v *jwtVerifier) fetch(ctx context.Context, url string) (*keySet, error) {
	ch := v.flights.DoChan(url, func() (any, error) {
		req, err := http.NewRequestWithContext(context.WithoutCancel(ctx), http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		resp, err := v.client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("fetch JWKS: %w", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("fetch JWKS: %s", resp.Status)
		}
		set, err := parseJWKS(io.LimitReader(resp.Body, 1<<20))
		if err != nil {
			return nil, err
		}
		v.sets.Store(url, set)
		return set, nil
	})
	select {
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.(*keySet), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func parseJWKS(r io.Reader) (*keySet, error) {
	var doc struct {
		Keys []struct {
			Kty, Kid, Use, Crv, N, E, X, Y string
		} `json:"keys"`
	}
	if err := json.NewDecoder(r).Decode(&doc); err != nil {
		return nil, fmt.Errorf("parse JWKS: %w", err)
	}
	set := &keySet{keys: map[string]any{}, fetched: time.Now()}
	b64 := base64.RawURLEncoding.DecodeString
	for _, k := range doc.Keys {
		if k.Use != "" && k.Use != "sig" {
			continue
		}
		switch k.Kty {
		case "RSA":
			n, err1 := b64(k.N)
			e, err2 := b64(k.E)
			if err1 != nil || err2 != nil || len(e) > 4 {
				continue
			}
			set.keys[k.Kid] = &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: int(new(big.Int).SetBytes(e).Int64())}
		case "EC":
			curve := map[string]elliptic.Curve{"P-256": elliptic.P256(), "P-384": elliptic.P384(), "P-521": elliptic.P521()}[k.Crv]
			x, err1 := b64(k.X)
			y, err2 := b64(k.Y)
			if curve == nil || err1 != nil || err2 != nil {
				continue
			}
			size := (curve.Params().BitSize + 7) / 8
			point := append([]byte{4}, append(pad(x, size), pad(y, size)...)...)
			if pub, err := ecdsa.ParseUncompressedPublicKey(curve, point); err == nil {
				set.keys[k.Kid] = pub
			}
		}
	}
	return set, nil
}

func pad(b []byte, size int) []byte {
	if len(b) >= size {
		return b
	}
	return append(make([]byte, size-len(b)), b...)
}
