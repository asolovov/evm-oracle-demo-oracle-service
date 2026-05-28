package config

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/viper"
)

// DecodeOption returns the viper.Unmarshal hook chain the oracle-service uses.
//
// Why this exists: viper.Unmarshal's default decoder chain is
//
//	mapstructure.StringToTimeDurationHookFunc()  +  StringToSliceHookFunc(",")
//
// Neither handles a JSON-encoded string -> map[string]string. That bites
// the moment an operator wires CHAIN_AGGREGATOR_ADDRESSES (or any other
// map-typed env var) as a JSON string:
//
//	export CHAIN_AGGREGATOR_ADDRESSES='{"WETH":"0x...","WBTC":"0x..."}'
//
// Without a hook viper hands the raw string to mapstructure, which then
// errors with "expected a map, got 'string'". Same problem for JSON-array
// slices (e.g. SIGNER_REPORTER_KEY_PATHS) — the default slice hook would
// split on ',' INSIDE the JSON and shred the array.
//
// JSONStringDecodeHook (defined below) catches strings whose first non-
// whitespace byte is '{' or '[' and JSON-decodes them. It's ordered FIRST
// in the chain so JSON-shaped strings win against the comma-split slice
// hook. Non-JSON strings fall through to the default hooks unchanged.
func DecodeOption() viper.DecoderConfigOption {
	return viper.DecodeHook(mapstructure.ComposeDecodeHookFunc(
		JSONStringDecodeHook(),
		mapstructure.StringToTimeDurationHookFunc(),
		mapstructure.StringToSliceHookFunc(","),
	))
}

// JSONStringDecodeHook decodes JSON-shaped strings into the target map or
// slice. Returns the input unchanged for non-JSON strings or non-collection
// targets, so the rest of the decoder chain still has a chance to act.
func JSONStringDecodeHook() mapstructure.DecodeHookFunc {
	return func(from, to reflect.Type, data interface{}) (interface{}, error) {
		if from.Kind() != reflect.String {
			return data, nil
		}
		if to.Kind() != reflect.Map && to.Kind() != reflect.Slice {
			return data, nil
		}
		s, ok := data.(string)
		if !ok {
			return data, nil
		}
		s = strings.TrimSpace(s)
		if s == "" {
			return data, nil
		}
		first := s[0]
		if first != '{' && first != '[' {
			return data, nil
		}
		var out interface{}
		if err := json.Unmarshal([]byte(s), &out); err != nil {
			return nil, fmt.Errorf("decode JSON-encoded %v from env: %w", to, err)
		}
		return out, nil
	}
}
