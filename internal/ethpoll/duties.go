package ethpoll

// Validator duty tracking (US-1.12) for the validators the operator lists.
// Duties are per epoch, so this runs on its own clock instead of the 1s
// health poll, and serves its own registry (DutiesHandler), scraped on the
// standard path: per-validator series at 1s would multiply ingest for
// values that change once per epoch.
//
// Every call is a read of public chain data, keyed by validator index:
//
//	GET  <beacon>/eth/v1/beacon/genesis, /eth/v1/config/spec   (once)
//	GET  <beacon>/eth/v1/beacon/states/head/validators?id=...  (each epoch)
//	GET  <beacon>/eth/v1/validator/duties/proposer/{epoch}     (each epoch)
//	POST <beacon>/eth/v1/validator/liveness/{epoch}            (each epoch)
//	POST <beacon>/eth/v1/beacon/rewards/attestations/{epoch}   (each epoch)
//	GET  <beacon>/eth/v1/beacon/headers/{slot}                 (own proposal slots)
//
// The two POSTs only carry the index list in the body; neither changes
// node state.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// MaxValidators caps --validators: one agent serves one node of a small
// operator (Increment 1), and each validator adds 8 series.
const MaxValidators = 1000

// dutyTimeout bounds one duty call; the rewards call makes the node replay
// an epoch, which is slower than the health endpoints.
const dutyTimeout = 10 * time.Second

// DutyCalls lists the duty requests, made only when validators are set.
var DutyCalls = []string{
	"GET <beacon>/eth/v1/beacon/genesis, /eth/v1/config/spec",
	"GET <beacon>/eth/v1/beacon/states/head/validators?id=<configured validators>",
	"GET <beacon>/eth/v1/validator/duties/proposer/{epoch}",
	"POST <beacon>/eth/v1/validator/liveness/{epoch} (body: validator indices)",
	"POST <beacon>/eth/v1/beacon/rewards/attestations/{epoch} (body: validator indices)",
	"GET <beacon>/eth/v1/beacon/headers/{slot} (own proposal slots)",
}

var pubkeyRe = regexp.MustCompile(`^0x[0-9a-fA-F]{96}$`)

// ParseValidators splits a comma/space separated list of validator indices
// and 0x-prefixed BLS pubkeys, dropping duplicates.
func ParseValidators(s string) ([]string, error) {
	var out []string
	seen := map[string]bool{}
	for _, v := range strings.FieldsFunc(s, func(r rune) bool { return r == ',' || r == ' ' || r == '\n' || r == '\t' }) {
		if _, err := strconv.ParseUint(v, 10, 64); err != nil && !pubkeyRe.MatchString(v) {
			return nil, fmt.Errorf("validator %q: want an index or a 0x-prefixed 48-byte pubkey", v)
		}
		v = strings.ToLower(v)
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	if len(out) > MaxValidators {
		return nil, fmt.Errorf("%d validators: at most %d per agent", len(out), MaxValidators)
	}
	return out, nil
}

type validator struct {
	effBalance string
	activation uint64
	exit       uint64
	streak     int
}

func (v *validator) activeIn(epoch uint64) bool {
	return v.activation <= epoch && epoch < v.exit
}

type duties struct {
	p   *Poller
	ids []string
	reg *prometheus.Registry

	genesis  time.Time
	slotDur  time.Duration
	perEpoch uint64

	vals      map[string]*validator // by index
	proposals map[uint64]string     // own proposal slot -> index

	// next epoch to process per check; 0 = start from the current one.
	nextDuties, nextLiveness, nextRewards uint64
	// Epoch after the last successful check; the cursors above may skip
	// epochs the API no longer answers, these do not (lag metric).
	doneLiveness, doneRewards uint64
	retryAt                   map[string]time.Time

	configured, unknown, activeN, missedLast prometheus.Gauge
	lag                                      *prometheus.GaugeVec
	rewardsSupported                         prometheus.Gauge
	active, slashed, balance, streak         *prometheus.GaugeVec
	attestations, proposed                   *prometheus.CounterVec
	earned, ideal                            *prometheus.CounterVec
}

func newDuties(p *Poller, ids []string) *duties {
	d := &duties{p: p, ids: ids, reg: prometheus.NewRegistry(),
		vals: map[string]*validator{}, proposals: map[uint64]string{}, retryAt: map[string]time.Time{}}
	cl := prometheus.Labels{"module": "validator", "node": "consensus"}
	gauge := func(name, help string) prometheus.Gauge {
		g := prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help, ConstLabels: cl})
		d.reg.MustRegister(g)
		return g
	}
	gaugeVec := func(name, help string, labels ...string) *prometheus.GaugeVec {
		g := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: help, ConstLabels: cl}, labels)
		d.reg.MustRegister(g)
		return g
	}
	counterVec := func(name, help string, labels ...string) *prometheus.CounterVec {
		c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help, ConstLabels: cl}, labels)
		d.reg.MustRegister(c)
		return c
	}
	d.configured = gauge("eth_validators_configured", "Validators listed in the agent config.")
	d.unknown = gauge("eth_validators_unknown", "Configured validators the beacon node does not know (typo, or not yet deposited).")
	d.activeN = gauge("eth_validators_active", "Configured validators active in the last checked epoch.")
	d.missedLast = gauge("eth_validators_missed_attestations_last_epoch", "Active configured validators with no attestation seen in the last checked epoch.")
	d.lag = gaugeVec("eth_validator_duties_lag_epochs", "Due epochs not yet checked successfully (0 = on schedule). Epochs the node could not answer in time are lost, not retried.", "check")
	d.rewardsSupported = gauge("eth_validator_rewards_api_supported", "0 if the beacon node does not implement the attestation rewards API.")
	d.active = gaugeVec("eth_validator_active", "1 if the validator is active in the current epoch.", "validator")
	d.slashed = gaugeVec("eth_validator_slashed", "1 if the validator has been slashed.", "validator")
	d.balance = gaugeVec("eth_validator_balance_gwei", "Validator balance at the head.", "validator")
	d.streak = gaugeVec("eth_validator_missed_attestation_streak", "Consecutive epochs with no attestation seen from the validator.", "validator")
	d.attestations = counterVec("eth_validator_attestations_total", "Attestation duties by liveness result (live|missed).", "validator", "result")
	d.proposed = counterVec("eth_validator_proposals_total", "Block proposal duties by result (proposed|missed).", "validator", "result")
	d.earned = counterVec("eth_validator_attestation_reward_gwei_total", "Head+target+source attestation rewards earned (penalties count as 0).", "validator")
	d.ideal = counterVec("eth_validator_attestation_ideal_reward_gwei_total", "Head+target+source rewards a perfect attestation would have earned.", "validator")
	d.configured.Set(float64(len(ids)))
	d.unknown.Set(float64(len(ids)))
	d.rewardsSupported.Set(1)
	for _, c := range []string{"liveness", "rewards", "proposals"} {
		d.lag.WithLabelValues(c).Set(0)
	}
	return d
}

// run ticks each second; each check does its work once per epoch (or per
// own proposal slot) and retries a failed call after one slot.
func (d *duties) run(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		d.tick(ctx, time.Now())
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (d *duties) tick(ctx context.Context, now time.Time) {
	if d.perEpoch == 0 {
		if !d.due("chain", now) || d.loadChain(ctx) != nil {
			return
		}
	}
	if now.Before(d.genesis) {
		return
	}
	slot := uint64(now.Sub(d.genesis) / d.slotDur)
	epoch := slot / d.perEpoch
	// Liveness and rewards wait a couple of slots into the epoch so
	// attestations from the previous epoch's last slot have arrived.
	settled := slot%d.perEpoch >= min(2, d.perEpoch-1)

	if d.nextDuties == 0 {
		d.nextDuties, d.nextLiveness, d.nextRewards = epoch, epoch, epoch
		d.doneLiveness, d.doneRewards = epoch, epoch
	}
	if d.nextDuties <= epoch && d.due("duties", now) {
		if d.refresh(ctx, epoch) == nil {
			d.nextDuties = epoch + 1
		} else {
			d.failed("duties", now)
		}
	}
	// Liveness is answered for the current and previous epoch only.
	if settled && epoch >= 1 && d.nextLiveness <= epoch-1 && d.due("liveness", now) {
		if d.nextLiveness < epoch-1 {
			d.nextLiveness = epoch - 1
		}
		if d.liveness(ctx, epoch-1) == nil {
			d.nextLiveness, d.doneLiveness = epoch, epoch
		} else {
			d.failed("liveness", now)
		}
	}
	// Attestations for epoch E can be included until the end of E+1, so
	// rewards for E are final from E+2.
	if settled && epoch >= 2 && d.nextRewards <= epoch-2 && d.due("rewards", now) {
		if d.nextRewards+1 < epoch-2 {
			d.nextRewards = epoch - 2 // missed epochs (agent down) are skipped
		}
		switch err := d.rewards(ctx, d.nextRewards); {
		case err == nil:
			d.nextRewards++
			d.doneRewards = d.nextRewards
		case errors.Is(err, errUnsupported):
			d.rewardsSupported.Set(0)
			d.nextRewards++
			d.doneRewards = d.nextRewards
		default:
			d.failed("rewards", now)
		}
	}
	// Own proposal slots: judged two slots later, when a late block would
	// have arrived.
	for s, idx := range d.proposals {
		if slot < s+2 || !d.due("proposal", now) {
			continue
		}
		switch ok, err := d.proposal(ctx, s, idx); {
		case err != nil:
			d.failed("proposal", now)
		default:
			if ok != nil {
				d.proposed.WithLabelValues(idx, map[bool]string{true: "proposed", false: "missed"}[*ok]).Inc()
			}
			delete(d.proposals, s)
		}
	}

	// Epochs that are due but not processed yet (0 while on schedule).
	// want is the epoch a check handles now; it is due once settled.
	lag := func(next, want uint64) float64 {
		due := int64(want)
		if !settled {
			due--
		}
		return float64(max(0, due-int64(next)+1))
	}
	if epoch >= 1 {
		d.lag.WithLabelValues("liveness").Set(lag(d.doneLiveness, epoch-1))
	}
	if epoch >= 2 {
		d.lag.WithLabelValues("rewards").Set(lag(d.doneRewards, epoch-2))
	}
	d.lag.WithLabelValues("proposals").Set(lag(d.nextDuties, epoch))
}

func (d *duties) due(check string, now time.Time) bool { return !now.Before(d.retryAt[check]) }

func (d *duties) failed(check string, now time.Time) {
	wait := d.slotDur
	if wait <= 0 {
		wait = 12 * time.Second
	}
	d.retryAt[check] = now.Add(wait)
}

func (d *duties) loadChain(ctx context.Context) error {
	var gen struct {
		Data struct {
			GenesisTime string `json:"genesis_time"`
		} `json:"data"`
	}
	var spec struct {
		Data map[string]any `json:"data"`
	}
	if err := d.get(ctx, "/eth/v1/beacon/genesis", &gen); err != nil {
		return err
	}
	if err := d.get(ctx, "/eth/v1/config/spec", &spec); err != nil {
		return err
	}
	gt, err1 := strconv.ParseInt(gen.Data.GenesisTime, 10, 64)
	sps, err2 := strconv.ParseUint(fmt.Sprint(spec.Data["SECONDS_PER_SLOT"]), 10, 64)
	spe, err3 := strconv.ParseUint(fmt.Sprint(spec.Data["SLOTS_PER_EPOCH"]), 10, 64)
	if err := errors.Join(err1, err2, err3); err != nil || sps == 0 || spe == 0 {
		return fmt.Errorf("chain parameters: %v", err)
	}
	d.genesis, d.slotDur, d.perEpoch = time.Unix(gt, 0), time.Duration(sps)*time.Second, spe
	return nil
}

// refresh resolves the configured ids to indices with their status, and
// loads this epoch's proposer duties.
func (d *duties) refresh(ctx context.Context, epoch uint64) error {
	type entry struct {
		Index     string `json:"index"`
		Balance   string `json:"balance"`
		Validator struct {
			Pubkey          string `json:"pubkey"`
			EffBalance      string `json:"effective_balance"`
			Slashed         bool   `json:"slashed"`
			ActivationEpoch string `json:"activation_epoch"`
			ExitEpoch       string `json:"exit_epoch"`
		} `json:"validator"`
	}
	found := map[string]bool{}
	for i := 0; i < len(d.ids); i += 30 {
		var out struct {
			Data []entry `json:"data"`
		}
		q := "id=" + strings.Join(d.ids[i:min(i+30, len(d.ids))], "&id=")
		if err := d.get(ctx, "/eth/v1/beacon/states/head/validators?"+q, &out); err != nil {
			return err
		}
		for _, e := range out.Data {
			found[e.Index], found[strings.ToLower(e.Validator.Pubkey)] = true, true
			v := d.vals[e.Index]
			if v == nil {
				v = &validator{}
				d.vals[e.Index] = v
				// Start counters at 0 so the first miss shows in increase().
				for _, r := range []string{"live", "missed"} {
					d.attestations.WithLabelValues(e.Index, r)
				}
				for _, r := range []string{"proposed", "missed"} {
					d.proposed.WithLabelValues(e.Index, r)
				}
				d.earned.WithLabelValues(e.Index)
				d.ideal.WithLabelValues(e.Index)
				d.streak.WithLabelValues(e.Index).Set(0)
			}
			v.effBalance = e.Validator.EffBalance
			v.activation, _ = strconv.ParseUint(e.Validator.ActivationEpoch, 10, 64)
			v.exit, _ = strconv.ParseUint(e.Validator.ExitEpoch, 10, 64)
			d.active.WithLabelValues(e.Index).Set(b2f(v.activeIn(epoch)))
			d.slashed.WithLabelValues(e.Index).Set(b2f(e.Validator.Slashed))
			if b, err := strconv.ParseFloat(e.Balance, 64); err == nil {
				d.balance.WithLabelValues(e.Index).Set(b)
			}
		}
	}
	unknown := 0
	for _, id := range d.ids {
		if !found[id] {
			unknown++
		}
	}
	d.unknown.Set(float64(unknown))
	if len(d.vals) == 0 {
		return nil
	}

	var duty struct {
		Data []struct {
			Index string `json:"validator_index"`
			Slot  string `json:"slot"`
		} `json:"data"`
	}
	if err := d.get(ctx, fmt.Sprintf("/eth/v1/validator/duties/proposer/%d", epoch), &duty); err != nil {
		return err
	}
	for _, e := range duty.Data {
		if s, err := strconv.ParseUint(e.Slot, 10, 64); err == nil && d.vals[e.Index] != nil {
			d.proposals[s] = e.Index
		}
	}
	return nil
}

// liveness asks whether the node saw an attestation (or block) from each
// active validator in epoch (validator-liveness API, as used by doppelganger
// protection). This counts gossip, so it holds even when the validator
// client talks to a different beacon node.
func (d *duties) liveness(ctx context.Context, epoch uint64) error {
	var ids []string
	for idx, v := range d.vals {
		if v.activeIn(epoch) {
			ids = append(ids, idx)
		}
	}
	d.activeN.Set(float64(len(ids)))
	if len(ids) == 0 {
		d.missedLast.Set(0)
		return nil
	}
	var out struct {
		Data []struct {
			Index  string `json:"index"`
			IsLive bool   `json:"is_live"`
		} `json:"data"`
	}
	if err := d.post(ctx, fmt.Sprintf("/eth/v1/validator/liveness/%d", epoch), ids, &out); err != nil {
		return err
	}
	missed := 0
	for _, e := range out.Data {
		v := d.vals[e.Index]
		if v == nil {
			continue
		}
		if e.IsLive {
			v.streak = 0
			d.attestations.WithLabelValues(e.Index, "live").Inc()
		} else {
			v.streak++
			missed++
			d.attestations.WithLabelValues(e.Index, "missed").Inc()
		}
		d.streak.WithLabelValues(e.Index).Set(float64(v.streak))
	}
	d.missedLast.Set(float64(missed))
	return nil
}

var errUnsupported = errors.New("not supported by this beacon node")

// rewards adds epoch's head+target+source attestation rewards against the
// ideal for each validator's effective balance. Penalties (negative
// components of a missed or wrong vote) count as 0 earned, so the ratio is
// the share of attainable reward. During an inactivity leak the ideal is 0
// and the epoch is skipped: effectiveness is undefined then.
func (d *duties) rewards(ctx context.Context, epoch uint64) error {
	var ids []string
	for idx, v := range d.vals {
		if v.activeIn(epoch) {
			ids = append(ids, idx)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	type parts struct {
		EffBalance string `json:"effective_balance"`
		Index      string `json:"validator_index"`
		Head       string `json:"head"`
		Target     string `json:"target"`
		Source     string `json:"source"`
	}
	var out struct {
		Data struct {
			Ideal []parts `json:"ideal_rewards"`
			Total []parts `json:"total_rewards"`
		} `json:"data"`
	}
	if err := d.post(ctx, fmt.Sprintf("/eth/v1/beacon/rewards/attestations/%d", epoch), ids, &out); err != nil {
		return err
	}
	d.rewardsSupported.Set(1)
	sum := func(p parts, clamp bool) float64 {
		t := 0.0
		for _, s := range []string{p.Head, p.Target, p.Source} {
			n, err := strconv.ParseFloat(s, 64)
			if err != nil || clamp && n < 0 {
				continue
			}
			t += n
		}
		return t
	}
	ideal := map[string]float64{}
	for _, p := range out.Data.Ideal {
		ideal[p.EffBalance] = sum(p, false)
	}
	for _, p := range out.Data.Total {
		v := d.vals[p.Index]
		if v == nil || ideal[v.effBalance] <= 0 {
			continue
		}
		d.ideal.WithLabelValues(p.Index).Add(ideal[v.effBalance])
		d.earned.WithLabelValues(p.Index).Add(min(sum(p, true), ideal[v.effBalance]))
	}
	return nil
}

// proposal reports whether the canonical block at slot came from idx. nil
// means the duty moved (a reorg changed the shuffling): not ours to judge.
func (d *duties) proposal(ctx context.Context, slot uint64, idx string) (*bool, error) {
	var out struct {
		Data struct {
			Header struct {
				Message struct {
					ProposerIndex string `json:"proposer_index"`
				} `json:"message"`
			} `json:"header"`
		} `json:"data"`
	}
	err := d.get(ctx, fmt.Sprintf("/eth/v1/beacon/headers/%d", slot), &out)
	if errors.Is(err, errNotFound) {
		missed := false
		return &missed, nil
	}
	if err != nil {
		return nil, err
	}
	if out.Data.Header.Message.ProposerIndex != idx {
		return nil, nil
	}
	ok := true
	return &ok, nil
}

var errNotFound = errors.New("not found")

func (d *duties) get(ctx context.Context, path string, out any) error {
	return d.do(ctx, http.MethodGet, path, nil, out)
}

func (d *duties) post(ctx context.Context, path string, body, out any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	return d.do(ctx, http.MethodPost, path, b, out)
}

func (d *duties) do(ctx context.Context, method, path string, body []byte, out any) error {
	ctx, cancel := context.WithTimeout(ctx, dutyTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, d.p.cfg.BeaconURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := d.p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		return json.NewDecoder(io.LimitReader(resp.Body, maxBody)).Decode(out)
	case http.StatusNotFound:
		if strings.HasPrefix(path, "/eth/v1/beacon/headers/") {
			return errNotFound
		}
		return fmt.Errorf("%s %s: %w", method, path, errUnsupported)
	case http.StatusMethodNotAllowed, http.StatusNotImplemented:
		return fmt.Errorf("%s %s: %w", method, path, errUnsupported)
	}
	return fmt.Errorf("%s %s: %s", method, path, resp.Status)
}

// DutiesHandler serves the duty series; nil when no validators are set.
func (p *Poller) DutiesHandler() http.Handler {
	if p.duties == nil {
		return nil
	}
	return promhttp.HandlerFor(p.duties.reg, promhttp.HandlerOpts{})
}
