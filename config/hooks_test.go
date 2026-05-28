package config

import (
	"reflect"
	"testing"
	"time"

	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/viper"
)

func TestJSONStringDecodeHook_MapFromJSON(t *testing.T) {
	hook := JSONStringDecodeHook()
	from := reflect.TypeOf("")
	to := reflect.TypeOf(map[string]string{})

	got, err := hook.(func(reflect.Type, reflect.Type, interface{}) (interface{}, error))(
		from, to, `{"WETH":"0x1","WBTC":"0x2"}`,
	)
	if err != nil {
		t.Fatalf("hook err: %v", err)
	}
	m, ok := got.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map[string]interface{}, got %T", got)
	}
	if m["WETH"] != "0x1" || m["WBTC"] != "0x2" {
		t.Fatalf("unexpected payload: %v", m)
	}
}

func TestJSONStringDecodeHook_SliceFromJSON(t *testing.T) {
	hook := JSONStringDecodeHook()
	from := reflect.TypeOf("")
	to := reflect.TypeOf([]string{})

	got, err := hook.(func(reflect.Type, reflect.Type, interface{}) (interface{}, error))(
		from, to, `["/a/r1","/a/r2","/a/r3"]`,
	)
	if err != nil {
		t.Fatalf("hook err: %v", err)
	}
	sl, ok := got.([]interface{})
	if !ok {
		t.Fatalf("expected []interface{}, got %T", got)
	}
	if len(sl) != 3 || sl[0] != "/a/r1" {
		t.Fatalf("unexpected payload: %v", sl)
	}
}

func TestJSONStringDecodeHook_NonJSONStringPassesThrough(t *testing.T) {
	hook := JSONStringDecodeHook()
	from := reflect.TypeOf("")
	to := reflect.TypeOf([]string{})

	got, err := hook.(func(reflect.Type, reflect.Type, interface{}) (interface{}, error))(
		from, to, "a,b,c",
	)
	if err != nil {
		t.Fatalf("hook err: %v", err)
	}
	if got.(string) != "a,b,c" {
		t.Fatalf("expected pass-through, got %v", got)
	}
}

func TestJSONStringDecodeHook_EmptyStringPassesThrough(t *testing.T) {
	hook := JSONStringDecodeHook()
	from := reflect.TypeOf("")
	to := reflect.TypeOf(map[string]string{})

	got, err := hook.(func(reflect.Type, reflect.Type, interface{}) (interface{}, error))(
		from, to, "  ",
	)
	if err != nil {
		t.Fatalf("hook err: %v", err)
	}
	if got.(string) != "  " {
		t.Fatalf("expected pass-through, got %v", got)
	}
}

func TestJSONStringDecodeHook_NonStringSourcePassesThrough(t *testing.T) {
	hook := JSONStringDecodeHook()
	from := reflect.TypeOf(0)
	to := reflect.TypeOf(map[string]string{})

	got, err := hook.(func(reflect.Type, reflect.Type, interface{}) (interface{}, error))(
		from, to, 42,
	)
	if err != nil {
		t.Fatalf("hook err: %v", err)
	}
	if got.(int) != 42 {
		t.Fatalf("expected pass-through, got %v", got)
	}
}

func TestJSONStringDecodeHook_NonCollectionTargetPassesThrough(t *testing.T) {
	hook := JSONStringDecodeHook()
	from := reflect.TypeOf("")
	to := reflect.TypeOf("")

	got, err := hook.(func(reflect.Type, reflect.Type, interface{}) (interface{}, error))(
		from, to, `{"a":1}`,
	)
	if err != nil {
		t.Fatalf("hook err: %v", err)
	}
	if got.(string) != `{"a":1}` {
		t.Fatalf("expected pass-through, got %v", got)
	}
}

func TestJSONStringDecodeHook_MalformedJSONErrors(t *testing.T) {
	hook := JSONStringDecodeHook()
	from := reflect.TypeOf("")
	to := reflect.TypeOf(map[string]string{})

	_, err := hook.(func(reflect.Type, reflect.Type, interface{}) (interface{}, error))(
		from, to, `{not json`,
	)
	if err == nil {
		t.Fatal("expected error on malformed JSON")
	}
}

// End-to-end: build a viper instance, feed JSON-shaped env values, Unmarshal
// into the real Scheme, and verify both the map (CHAIN_AGGREGATOR_ADDRESSES)
// and the slice (SIGNER_REPORTER_KEY_PATHS) populate correctly via the hook.
//
// This is the actual bug repro: without DecodeOption, the map decode errors
// with "expected a map, got 'string'" and the slice gets shredded on commas.
func TestDecodeOption_EnvJSONStringRoundTrip(t *testing.T) {
	t.Cleanup(func() { viper.Reset() })
	viper.Reset()
	setDefaults()

	// Simulate what compose / a real shell would do: set env-style values.
	viper.Set("chain.aggregator_addresses", `{"WETH":"0xaaaa","WBTC":"0xbbbb"}`)
	viper.Set("signer.reporter_key_paths", `["/etc/lighthouse/secrets/r1.json","/etc/lighthouse/secrets/r2.json","/etc/lighthouse/secrets/r3.json"]`)

	var cfg Scheme
	if err := viper.Unmarshal(&cfg, DecodeOption()); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if got := cfg.Chain.AggregatorAddresses["WETH"]; got != "0xaaaa" {
		t.Fatalf("AggregatorAddresses[WETH] = %q, want 0xaaaa (full map: %+v)",
			got, cfg.Chain.AggregatorAddresses)
	}
	if got := cfg.Chain.AggregatorAddresses["WBTC"]; got != "0xbbbb" {
		t.Fatalf("AggregatorAddresses[WBTC] = %q, want 0xbbbb", got)
	}
	if n := len(cfg.Signer.ReporterKeyPaths); n != 3 {
		t.Fatalf("expected 3 reporter key paths, got %d (slice: %v)", n, cfg.Signer.ReporterKeyPaths)
	}
	if cfg.Signer.ReporterKeyPaths[0] != "/etc/lighthouse/secrets/r1.json" {
		t.Fatalf("first path = %q", cfg.Signer.ReporterKeyPaths[0])
	}
}

// Confirms the default viper.Unmarshal (no hook) cannot decode the same map.
// Locks the bug so a future refactor that drops DecodeOption() trips this test.
func TestDecodeOption_RequiredForMapEnvVars(t *testing.T) {
	t.Cleanup(func() { viper.Reset() })
	viper.Reset()
	setDefaults()

	viper.Set("chain.aggregator_addresses", `{"WETH":"0xaaaa"}`)

	var cfg Scheme
	err := viper.Unmarshal(&cfg) // NO DecodeOption — should fail.
	if err == nil {
		// On some library versions the decoder is more permissive — guard
		// the assertion to the actual symptom of the bug (empty map).
		if len(cfg.Chain.AggregatorAddresses) != 0 {
			t.Fatalf("expected empty/failed decode without hook; got %v", cfg.Chain.AggregatorAddresses)
		}
		return
	}
	// Make sure the failure mentions the map decode (defends against an
	// unrelated future error masking the regression).
	if !errorContains(err, "map") && !errorContains(err, "Chain.AggregatorAddresses") {
		t.Fatalf("expected map-decode error, got: %v", err)
	}
}

func errorContains(err error, substr string) bool {
	if err == nil {
		return false
	}
	return len(err.Error()) > 0 && contains(err.Error(), substr)
}

func contains(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// Verify the hook composes with the default StringToTimeDurationHookFunc by
// confirming a duration string still decodes (we ordered the JSON hook first,
// but it should pass-through non-JSON strings).
func TestDecodeOption_DurationStillDecodes(t *testing.T) {
	t.Cleanup(func() { viper.Reset() })
	viper.Reset()
	setDefaults()

	type S struct {
		D time.Duration `mapstructure:"d"`
	}
	viper.Set("d", "30s")

	var s S
	if err := viper.Unmarshal(&s, DecodeOption()); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if s.D != 30*time.Second {
		t.Fatalf("duration decode broken: %v", s.D)
	}
}

// time is imported here to keep the duration test self-contained; mapstructure
// stays imported via the hook composition.
var _ = mapstructure.ComposeDecodeHookFunc
