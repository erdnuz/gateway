package hub

import (
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"gateway/packages/common/types"
)

func validGatewayConfig() *types.GatewayConfig {
	return &types.GatewayConfig{
		Runtime: types.DefaultRuntimePolicy(),
		Prefixes: []types.PrefixConfig{
			{
				Prefix:      "v1",
				QuotaPeriod: time.Hour,
				Services: []types.ServiceConfig{
					{
						ServiceID: "auth-api",
						TargetURL: "https://httpbin.org/anything",
						Tiers: []types.TierConfig{
							{TierID: "free", Quota: 100, GetCost: 1, PostCost: 1, PutCost: 1, DeleteCost: 1, OtherCost: 1},
						},
						Analytics: types.AnalyticsConfig{Enabled: true, SamplingRate: 1.0},
						Cache:     &types.CacheConfig{Enabled: true, TTL: 5 * time.Minute, CacheKey: "$method:$path:$key"},
						Failure: types.FailureConfig{
							Hub:      types.HubFailurePolicy{TierLookupStrategy: "stale-or-default", DefaultTier: "free", StaleTierMaxAge: time.Hour},
							Upstream: types.UpstreamFailurePolicy{Mode: "fail-closed", RetryOnStatuses: []int{502, 503, 504}},
						},
					},
				},
			},
		},
	}
}

func TestValidateGatewayConfig_Valid(t *testing.T) {
	cfg := validGatewayConfig()
	if err := ValidateGatewayConfig(cfg); err != nil {
		t.Fatalf("expected valid config, got err: %v", err)
	}
}

func TestValidateGatewayConfig_DuplicatePrefix(t *testing.T) {
	cfg := validGatewayConfig()
	cfg.Prefixes = append(cfg.Prefixes, cfg.Prefixes[0])

	if err := ValidateGatewayConfig(cfg); err == nil {
		t.Fatal("expected error for duplicate prefix")
	}
}

func TestValidateGatewayConfig_InvalidTargetURL(t *testing.T) {
	cfg := validGatewayConfig()
	cfg.Prefixes[0].Services[0].TargetURL = "not-a-url"

	if err := ValidateGatewayConfig(cfg); err == nil {
		t.Fatal("expected error for invalid target_url")
	}
}

func TestValidateGatewayConfig_InvalidHubTierStrategy(t *testing.T) {
	cfg := validGatewayConfig()
	cfg.Prefixes[0].Services[0].Failure.Hub.TierLookupStrategy = "maybe-open"

	if err := ValidateGatewayConfig(cfg); err == nil {
		t.Fatal("expected error for invalid tier_lookup_strategy")
	}
}

func TestValidateGatewayConfig_InvalidRetryStatusCode(t *testing.T) {
	cfg := validGatewayConfig()
	cfg.Prefixes[0].Services[0].Failure.Upstream.RetryOnStatuses = []int{42}

	if err := ValidateGatewayConfig(cfg); err == nil {
		t.Fatal("expected error for invalid retry status code")
	}
}

func TestT_CFG_01_OrphanFieldRegistry(t *testing.T) {
	allPaths := map[string]struct{}{}
	collectJSONLeafPaths(reflect.TypeOf(types.GatewayConfig{}), "", allPaths)

	coveredRoots := []string{
		"runtime.hub",
		"runtime.edge",
		"runtime.analytics",
		"prefixes",
	}

	var orphans []string
	for p := range allPaths {
		matched := false
		for _, root := range coveredRoots {
			if p == root || strings.HasPrefix(p, root+".") {
				matched = true
				break
			}
		}
		if !matched {
			orphans = append(orphans, p)
		}
	}

	sort.Strings(orphans)
	if len(orphans) > 0 {
		t.Fatalf("T-CFG-01 failed: found unregistered config fields: %v", orphans)
	}
}

func TestT_CFG_02_SchemaToRuntimeParity(t *testing.T) {
	effective := (types.RuntimePolicy{}).Effective()
	defaults := types.DefaultRuntimePolicy()
	if !reflect.DeepEqual(effective, defaults) {
		t.Fatalf("T-CFG-02 failed: RuntimePolicy{}.Effective() diverges from DefaultRuntimePolicy()")
	}

	assertNonZeroNonBoolFields(t, "runtime.hub", reflect.ValueOf(defaults.Hub))
	assertNonZeroNonBoolFields(t, "runtime.edge", reflect.ValueOf(defaults.Edge))
	assertNonZeroNonBoolFields(t, "runtime.analytics", reflect.ValueOf(defaults.Analytics))
}

func collectJSONLeafPaths(t reflect.Type, prefix string, out map[string]struct{}) {
	for t.Kind() == reflect.Pointer || t.Kind() == reflect.Slice || t.Kind() == reflect.Array {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		if prefix != "" {
			out[prefix] = struct{}{}
		}
		return
	}

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		name := strings.Split(f.Tag.Get("json"), ",")[0]
		if name == "" || name == "-" {
			continue
		}
		next := name
		if prefix != "" {
			next = prefix + "." + name
		}

		ft := f.Type
		for ft.Kind() == reflect.Pointer || ft.Kind() == reflect.Slice || ft.Kind() == reflect.Array {
			ft = ft.Elem()
		}
		if ft.Kind() == reflect.Struct && ft.PkgPath() != "time" {
			collectJSONLeafPaths(ft, next, out)
			continue
		}
		out[next] = struct{}{}
	}
}

func assertNonZeroNonBoolFields(t *testing.T, path string, v reflect.Value) {
	t.Helper()
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			t.Fatalf("%s must not be nil", path)
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return
	}

	vt := v.Type()
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		ft := vt.Field(i)
		if !ft.IsExported() {
			continue
		}
		name := strings.Split(ft.Tag.Get("json"), ",")[0]
		if name == "" || name == "-" {
			name = strings.ToLower(ft.Name)
		}
		next := path + "." + name

		for f.Kind() == reflect.Pointer {
			if f.IsNil() {
				t.Fatalf("%s must not be nil", next)
			}
			f = f.Elem()
		}

		switch f.Kind() {
		case reflect.Struct:
			assertNonZeroNonBoolFields(t, next, f)
		case reflect.Bool:
			continue
		case reflect.String:
			if strings.TrimSpace(f.String()) == "" {
				t.Fatalf("%s must be non-empty", next)
			}
		case reflect.Slice, reflect.Array, reflect.Map:
			if f.Len() == 0 {
				t.Fatalf("%s must be non-empty", next)
			}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if f.Int() == 0 {
				t.Fatalf("%s must be non-zero", next)
			}
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			if f.Uint() == 0 {
				t.Fatalf("%s must be non-zero", next)
			}
		case reflect.Float32, reflect.Float64:
			if f.Float() == 0 {
				t.Fatalf("%s must be non-zero", next)
			}
		}
	}
}
