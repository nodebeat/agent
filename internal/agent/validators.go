package agent

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/nodebeat/agent/internal/detect"
	"github.com/nodebeat/agent/internal/ethpoll"
)

// SplitValidators splits a comma/whitespace separated --validators value.
// Format checks need the chain, so they happen in NormalizeValidators once
// it is known (flag parsing runs before detection).
func SplitValidators(s string) []string {
	return strings.FieldsFunc(s, func(r rune) bool { return r == ',' || r == ' ' || r == '\n' || r == '\t' })
}

// NormalizeValidators validates the configured validators for chain and
// returns them deduplicated, in the form the collector takes:
//   - ethereum: indices or 0x pubkeys (ethpoll.ParseValidator)
//   - cosmos: consensus addresses as 40 upper-case hex characters, the form
//     cosmos-validator-watcher --validator and CometBFT /validators use;
//     bech32 consensus addresses (cosmosvalcons1..., osmovalcons1...) are
//     converted. Operator addresses (valoper) are not accepted: mapping them
//     needs the staking module's consensus pubkey.
//
// Idempotent, so it can re-run on already-normalized values.
func NormalizeValidators(chain string, raw []string) ([]string, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var parse func(string) (string, error)
	switch chain {
	case detect.ChainEthereum:
		parse = ethpoll.ParseValidator
	case detect.ChainCosmos:
		parse = cosmosConsensusAddress
	default:
		return nil, fmt.Errorf("--validators is not supported for chain %q", chain)
	}
	var out []string
	seen := map[string]bool{}
	for _, v := range raw {
		id, err := parse(v)
		if err != nil {
			return nil, fmt.Errorf("validator %q: %w", v, err)
		}
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	if len(out) > ethpoll.MaxValidators {
		return nil, fmt.Errorf("%d validators: at most %d per agent", len(out), ethpoll.MaxValidators)
	}
	return out, nil
}

func cosmosConsensusAddress(v string) (string, error) {
	if b, err := hex.DecodeString(v); err == nil && len(b) == 20 {
		return strings.ToUpper(v), nil
	}
	hrp, data, err := bech32Decode(v)
	if err != nil {
		return "", errors.New("want a consensus address: 40 hex characters or bech32 ...valcons1...")
	}
	if !strings.HasSuffix(hrp, "valcons") {
		return "", fmt.Errorf("bech32 prefix %q is not a consensus address (...valcons); operator (valoper) and account addresses are not accepted", hrp)
	}
	b, err := convertBits(data, 5, 8, false)
	if err != nil || len(b) != 20 {
		return "", errors.New("bech32 consensus address must hold 20 bytes")
	}
	return strings.ToUpper(hex.EncodeToString(b)), nil
}

// bech32Decode decodes and checksum-verifies a BIP-173 bech32 string.
func bech32Decode(s string) (string, []byte, error) {
	if len(s) > 90 || strings.ToLower(s) != s && strings.ToUpper(s) != s {
		return "", nil, errors.New("invalid bech32")
	}
	s = strings.ToLower(s)
	pos := strings.LastIndexByte(s, '1')
	if pos < 1 || pos+7 > len(s) {
		return "", nil, errors.New("invalid bech32 separator")
	}
	const charset = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"
	hrp := s[:pos]
	data := make([]byte, 0, len(s)-pos-1)
	for _, c := range s[pos+1:] {
		i := strings.IndexRune(charset, c)
		if i < 0 {
			return "", nil, errors.New("invalid bech32 character")
		}
		data = append(data, byte(i))
	}
	values := make([]byte, 0, 2*len(hrp)+1+len(data))
	for _, c := range hrp {
		values = append(values, byte(c>>5))
	}
	values = append(values, 0)
	for _, c := range hrp {
		values = append(values, byte(c&31))
	}
	values = append(values, data...)
	if bech32Polymod(values) != 1 {
		return "", nil, errors.New("invalid bech32 checksum")
	}
	return hrp, data[:len(data)-6], nil
}

func bech32Polymod(values []byte) uint32 {
	gen := [5]uint32{0x3b6a57b2, 0x26508e6d, 0x1ea119fa, 0x3d4233dd, 0x2a1462b3}
	chk := uint32(1)
	for _, v := range values {
		top := chk >> 25
		chk = (chk&0x1ffffff)<<5 ^ uint32(v)
		for i := range 5 {
			if (top>>i)&1 == 1 {
				chk ^= gen[i]
			}
		}
	}
	return chk
}

func convertBits(data []byte, from, to uint, pad bool) ([]byte, error) {
	var acc, bits uint
	maxv := uint(1)<<to - 1
	var out []byte
	for _, v := range data {
		acc = acc<<from | uint(v)
		bits += from
		for bits >= to {
			bits -= to
			out = append(out, byte(acc>>bits&maxv))
		}
	}
	if pad && bits > 0 {
		out = append(out, byte(acc<<(to-bits)&maxv))
	} else if !pad && (bits >= from || acc<<(to-bits)&maxv != 0) {
		return nil, errors.New("invalid padding")
	}
	return out, nil
}
