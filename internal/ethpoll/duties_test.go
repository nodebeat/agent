package ethpoll

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeChain is a beacon node with 8-slot epochs of 6s (the minimal preset
// the devnet runs), genesis at t0, and scripted duty answers.
type fakeChain struct {
	mu        sync.Mutex
	live      map[string]bool   // index -> is_live, for any epoch
	proposer  map[uint64]string // slot -> proposer index (duties)
	blocks    map[uint64]string // slot -> proposer of the canonical block
	earned    map[string]int64  // index -> head+target+source actual (split evenly)
	idealZero bool              // inactivity leak
	noRewards bool              // client without the rewards API
	calls     []string
}

var t0 = time.Unix(1_700_000_000, 0)

const pubkey0 = "0xaaf6c1251e73fb600624937760fef218aace5b253bf068ed45398aeb29d821e4d2899343ddcbbe37cb3f6cf500dff26c"

func (f *fakeChain) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.calls = append(f.calls, r.Method+" "+r.URL.Path)
		var ids []string
		if r.Method == http.MethodPost {
			if err := json.NewDecoder(r.Body).Decode(&ids); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
		}
		p := r.URL.Path
		switch {
		case p == "/eth/v1/beacon/genesis":
			fmt.Fprintf(w, `{"data":{"genesis_time":"%d"}}`, t0.Unix())
		case p == "/eth/v1/config/spec":
			io.WriteString(w, `{"data":{"SECONDS_PER_SLOT":"6","SLOTS_PER_EPOCH":"8"}}`)
		case p == "/eth/v1/beacon/states/head/validators":
			var out []string
			for _, id := range r.URL.Query()["id"] {
				idx := id
				if id == pubkey0 {
					idx = "0"
				}
				if idx == "99999" {
					continue
				}
				out = append(out, fmt.Sprintf(`{"index":"%s","balance":"32000000001","status":"active_ongoing","validator":{"pubkey":"%s","effective_balance":"32000000000","slashed":false,"activation_epoch":"0","exit_epoch":"18446744073709551615"}}`, idx, map[bool]string{true: pubkey0, false: "0x00"}[idx == "0"]))
			}
			fmt.Fprintf(w, `{"data":[%s]}`, strings.Join(out, ","))
		case strings.HasPrefix(p, "/eth/v1/validator/duties/proposer/"):
			var epoch uint64
			fmt.Sscan(strings.TrimPrefix(p, "/eth/v1/validator/duties/proposer/"), &epoch)
			var out []string
			for s, idx := range f.proposer {
				if s/8 != epoch {
					continue
				}
				out = append(out, fmt.Sprintf(`{"pubkey":"0x00","validator_index":"%s","slot":"%d"}`, idx, s))
			}
			fmt.Fprintf(w, `{"dependent_root":"0x00","data":[%s]}`, strings.Join(out, ","))
		case strings.HasPrefix(p, "/eth/v1/validator/liveness/"):
			var out []string
			for _, id := range ids {
				out = append(out, fmt.Sprintf(`{"index":"%s","is_live":%t}`, id, f.live[id]))
			}
			fmt.Fprintf(w, `{"data":[%s]}`, strings.Join(out, ","))
		case strings.HasPrefix(p, "/eth/v1/beacon/rewards/attestations/"):
			if f.noRewards {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			ideal := `{"effective_balance":"32000000000","head":"300","target":"400","source":"300","inactivity":"0"}`
			if f.idealZero {
				ideal = `{"effective_balance":"32000000000","head":"0","target":"0","source":"0","inactivity":"0"}`
			}
			var out []string
			for _, id := range ids {
				e := f.earned[id]
				out = append(out, fmt.Sprintf(`{"validator_index":"%s","head":"%d","target":"%d","source":"%d","inactivity":"0"}`, id, e*3/10, e*4/10, e*3/10))
			}
			fmt.Fprintf(w, `{"data":{"ideal_rewards":[%s],"total_rewards":[%s]}}`, ideal, strings.Join(out, ","))
		case strings.HasPrefix(p, "/eth/v1/beacon/headers/"):
			var slot uint64
			fmt.Sscan(strings.TrimPrefix(p, "/eth/v1/beacon/headers/"), &slot)
			idx, ok := f.blocks[slot]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				io.WriteString(w, `{"code":404,"message":"NOT_FOUND: beacon block at slot"}`)
				return
			}
			fmt.Fprintf(w, `{"data":{"header":{"message":{"slot":"%d","proposer_index":"%s"}}}}`, slot, idx)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
}

func setupDuties(t *testing.T, f *fakeChain, ids ...string) *duties {
	t.Helper()
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)
	p := New(Config{BeaconURL: srv.URL, Validators: ids})
	if p.duties == nil || p.DutiesHandler() == nil {
		t.Fatal("duties not enabled")
	}
	return p.duties
}

// at returns the wall time of slot s.
func at(slot uint64) time.Time { return t0.Add(time.Duration(slot) * 6 * time.Second) }

// runSlots ticks every slot from..to (inclusive), 3s into each slot.
func runSlots(d *duties, from, to uint64) {
	for s := from; s <= to; s++ {
		d.tick(context.Background(), at(s).Add(3*time.Second))
	}
}

func scrapeDuties(t *testing.T, d *duties) string {
	t.Helper()
	srv := httptest.NewServer(d.p.DutiesHandler())
	defer srv.Close()
	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

const vl = `module="validator",node="consensus"`

func TestParseValidator(t *testing.T) {
	if got, err := ParseValidator("0X" + strings.ToUpper(pubkey0[2:])); err == nil {
		t.Fatalf("0X prefix accepted: %q", got)
	}
	for in, want := range map[string]string{"12": "12", "0x" + strings.ToUpper(pubkey0[2:]): pubkey0} {
		if got, err := ParseValidator(in); err != nil || got != want {
			t.Errorf("ParseValidator(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, bad := range []string{"", "-1", "0x1234", "validator-1", "0x" + strings.Repeat("g", 96)} {
		if _, err := ParseValidator(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func TestNoValidatorsNoDuties(t *testing.T) {
	p := New(Config{BeaconURL: "http://127.0.0.1:1"})
	if p.duties != nil || p.DutiesHandler() != nil {
		t.Fatal("duties enabled without validators")
	}
}

// A validator that stops attesting builds a streak; one live epoch resets
// it. Pubkeys resolve to indices; unknown ids are counted, not fatal.
func TestMissedAttestationStreak(t *testing.T) {
	f := &fakeChain{live: map[string]bool{"0": true, "1": true}}
	d := setupDuties(t, f, pubkey0, "1", "99999")
	runSlots(d, 80, 87) // epoch 10: start; nothing judged yet
	body := scrapeDuties(t, d)
	mustHave(t, body,
		`eth_validators_configured{`+vl+`} 3`,
		`eth_validators_unknown{`+vl+`} 1`,
		`eth_validator_attestations_total{`+vl+`,result="missed",validator="0"} 0`,
	)

	f.mu.Lock()
	f.live["1"] = false
	f.mu.Unlock()
	runSlots(d, 88, 111) // epochs 11-13 judge 10-12
	mustHave(t, scrapeDuties(t, d),
		`eth_validator_missed_attestation_streak{`+vl+`,validator="1"} 3`,
		`eth_validator_missed_attestation_streak{`+vl+`,validator="0"} 0`,
		`eth_validator_attestations_total{`+vl+`,result="missed",validator="1"} 3`,
		`eth_validator_attestations_total{`+vl+`,result="live",validator="0"} 3`,
		`eth_validators_active{`+vl+`} 2`,
		`eth_validators_missed_attestations_last_epoch{`+vl+`} 1`,
		`eth_validator_duties_lag_epochs{check="liveness",`+vl+`} 0`,
	)

	f.mu.Lock()
	f.live["1"] = true
	f.mu.Unlock()
	runSlots(d, 112, 119)
	mustHave(t, scrapeDuties(t, d),
		`eth_validator_missed_attestation_streak{`+vl+`,validator="1"} 0`,
		`eth_validators_missed_attestations_last_epoch{`+vl+`} 0`,
	)
}

// Liveness for an epoch is asked once, two slots into the next epoch
// (slots 90, 98, 106 here).
func TestLivenessOncePerEpochAfterSettling(t *testing.T) {
	f := &fakeChain{live: map[string]bool{"1": true}}
	d := setupDuties(t, f, "1")
	runSlots(d, 80, 106)
	var got []string
	for _, c := range f.calls {
		if strings.Contains(c, "liveness") {
			got = append(got, c)
		}
	}
	if strings.Join(got, "|") != "POST /eth/v1/validator/liveness/10|POST /eth/v1/validator/liveness/11|POST /eth/v1/validator/liveness/12" {
		t.Fatalf("liveness calls = %q", got)
	}
}

// Effectiveness counters: earned (penalties as 0) against ideal; skipped
// during an inactivity leak; a client without the API is flagged.
func TestAttestationRewards(t *testing.T) {
	f := &fakeChain{live: map[string]bool{}, earned: map[string]int64{"1": 1000, "2": 500, "3": -700}}
	d := setupDuties(t, f, "1", "2", "3")
	runSlots(d, 80, 103) // epochs 10-12: rewards for epoch 10 at epoch 12
	mustHave(t, scrapeDuties(t, d),
		`eth_validator_attestation_ideal_reward_gwei_total{`+vl+`,validator="1"} 1000`,
		`eth_validator_attestation_reward_gwei_total{`+vl+`,validator="1"} 1000`,
		`eth_validator_attestation_reward_gwei_total{`+vl+`,validator="2"} 500`,
		`eth_validator_attestation_reward_gwei_total{`+vl+`,validator="3"} 0`,
		`eth_validator_attestation_ideal_reward_gwei_total{`+vl+`,validator="3"} 1000`,
	)

	f.mu.Lock()
	f.idealZero = true
	f.mu.Unlock()
	runSlots(d, 104, 111)
	mustHave(t, scrapeDuties(t, d),
		`eth_validator_attestation_ideal_reward_gwei_total{`+vl+`,validator="1"} 1000`,
		`eth_validator_duties_lag_epochs{check="rewards",`+vl+`} 0`,
	)

	f.mu.Lock()
	f.noRewards = true
	f.mu.Unlock()
	runSlots(d, 112, 119)
	mustHave(t, scrapeDuties(t, d),
		`eth_validator_rewards_api_supported{`+vl+`} 0`,
		`eth_validator_duties_lag_epochs{check="rewards",`+vl+`} 0`,
	)
}

// Own proposal slots: a canonical block by us is proposed, no block is
// missed, a block by someone else (duty moved) is not counted.
func TestProposals(t *testing.T) {
	f := &fakeChain{live: map[string]bool{},
		proposer: map[uint64]string{82: "1", 84: "2", 86: "1", 85: "7"},
		blocks:   map[uint64]string{82: "1", 86: "5"}}
	d := setupDuties(t, f, "1", "2")
	runSlots(d, 80, 85)
	mustHave(t, scrapeDuties(t, d),
		`eth_validator_proposals_total{`+vl+`,result="proposed",validator="1"} 1`,
		`eth_validator_proposals_total{`+vl+`,result="missed",validator="2"} 0`,
	)
	runSlots(d, 86, 90)
	mustHave(t, scrapeDuties(t, d),
		`eth_validator_proposals_total{`+vl+`,result="proposed",validator="1"} 1`,
		`eth_validator_proposals_total{`+vl+`,result="missed",validator="1"} 0`,
		`eth_validator_proposals_total{`+vl+`,result="missed",validator="2"} 1`,
	)
	if len(d.proposals) != 0 {
		t.Fatalf("pending proposals left: %v", d.proposals)
	}
}

// A beacon node that fails is retried after a slot and the lag shows how
// many whole epochs a check fell behind.
func TestLagWhileBeaconFails(t *testing.T) {
	f := &fakeChain{live: map[string]bool{"1": true}}
	d := setupDuties(t, f, "1")
	runSlots(d, 80, 90)
	d.p.cfg.BeaconURL = "http://127.0.0.1:1"
	runSlots(d, 91, 115)
	mustHave(t, scrapeDuties(t, d), `eth_validator_duties_lag_epochs{check="liveness",`+vl+`} 3`)
}
