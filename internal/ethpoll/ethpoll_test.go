package ethpoll

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeNode struct {
	mu        sync.Mutex
	head      int
	down      bool // beacon and EL refuse with 503
	hang      bool // beacon hangs until the request context ends
	elSyncing bool
	reorgs    chan string
}

func (f *fakeNode) beacon() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/eth/v1/node/syncing", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		down, hang, head := f.down, f.hang, f.head
		f.mu.Unlock()
		if hang {
			<-r.Context().Done()
			return
		}
		if down {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintf(w, `{"data":{"head_slot":"%d","sync_distance":"2","is_syncing":false,"is_optimistic":false,"el_offline":false}}`, head)
	})
	mux.HandleFunc("/eth/v1/node/peer_count", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"data":{"disconnected":"12","connecting":"1","connected":"50","disconnecting":"0"}}`)
	})
	mux.HandleFunc("/eth/v1/events", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("topics") != "chain_reorg" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.(http.Flusher).Flush()
		for {
			select {
			case <-r.Context().Done():
				return
			case depth := <-f.reorgs:
				fmt.Fprintf(w, "event: head\ndata: {\"slot\":\"9\"}\n\nevent: chain_reorg\ndata: {\"slot\":\"10\",\"depth\":\"%s\"}\n\n", depth)
				w.(http.Flusher).Flush()
			}
		}
	})
	return mux
}

func (f *fakeNode) execution() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		down, head, syncing := f.down, f.head, f.elSyncing
		f.mu.Unlock()
		if down {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		var batch []rpcReq
		if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var out []string
		for _, q := range batch {
			switch q.Method {
			case "eth_syncing":
				if syncing {
					out = append(out, fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"result":{"startingBlock":"0x0","currentBlock":"0x32","highestBlock":"0xc8"}}`, q.ID))
				} else {
					out = append(out, fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"result":false}`, q.ID))
				}
			case "eth_getBlockByNumber":
				out = append(out, fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"result":{"number":"0x%x","transactions":["0xaa","0xbb","0xcc"]}}`, q.ID, head))
			case "net_peerCount":
				// Namespace disabled: must not mark the EL down.
				out = append(out, fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"error":{"code":-32601,"message":"method not found"}}`, q.ID))
			case "eth_gasPrice":
				out = append(out, fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"result":"0x1dcd65000"}`, q.ID)) // 8 gwei
			}
		}
		io.WriteString(w, "["+strings.Join(out, ",")+"]")
	})
}

func setup(t *testing.T) (*fakeNode, *Poller) {
	t.Helper()
	f := &fakeNode{head: 158, reorgs: make(chan string, 4)}
	b := httptest.NewServer(f.beacon())
	e := httptest.NewServer(f.execution())
	t.Cleanup(func() { b.CloseClientConnections(); b.Close(); e.Close() })
	p := New(Config{BeaconURL: b.URL + "/", ExecutionURL: e.URL, Timeout: 300 * time.Millisecond})
	return f, p
}

func scrape(t *testing.T, p *Poller) string {
	t.Helper()
	rec := httptest.NewRecorder()
	p.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	return rec.Body.String()
}

func mustHave(t *testing.T, body string, lines ...string) {
	t.Helper()
	for _, l := range lines {
		if !strings.Contains(body, l+"\n") {
			t.Errorf("missing %q in:\n%s", l, body)
		}
	}
}

func TestPollHealthyNode(t *testing.T) {
	_, p := setup(t)
	p.PollOnce(context.Background())
	mustHave(t, scrape(t, p),
		`eth_con_health_up{module="health",node="consensus"} 1`,
		`eth_con_sync_head_slot{module="sync",node="consensus"} 158`,
		`eth_con_sync_distance{module="sync",node="consensus"} 2`,
		`eth_con_sync_is_syncing{module="sync",node="consensus"} 0`,
		`eth_con_peers{module="general",node="consensus",state="connected"} 50`,
		`eth_con_peers{module="general",node="consensus",state="disconnected"} 12`,
		`eth_exe_health_up{ethereum_role="execution",module="health",node_name="execution"} 1`,
		`eth_exe_block_most_recent_number{ethereum_role="execution",identifier="head",module="block",node_name="execution"} 158`,
		`eth_exe_block_head_transactions_in_block{ethereum_role="execution",module="block",node_name="execution"} 3`,
		`eth_exe_gas_price_gwei{ethereum_role="execution",module="general",node_name="execution"} 8`,
		`eth_exe_sync_is_syncing{ethereum_role="execution",module="sync",node_name="execution"} 0`,
		`eth_exe_sync_percentage{ethereum_role="execution",module="sync",node_name="execution"} 100`,
	)
}

// Rules and dashboards were written against ethereum-metrics-exporter; every
// series they read must exist with the exporter's label set, or upgraded
// agents would start new series and page EthereumTelemetryStale.
func TestSeriesMatchExporterSample(t *testing.T) {
	_, p := setup(t)
	p.PollOnce(context.Background())
	got := scrape(t, p)

	// Captured from ethereum-metrics-exporter v0.29.2 on the devnet (copy of
	// deploy/devnet/ethereum/samples/, kept here so the OSS export tests it).
	f, err := os.Open("testdata/exporter-geth-lighthouse.metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	consumed := regexp.MustCompile(`^(eth_con_(health_up|sync_head_slot|sync_distance|sync_is_syncing|sync_percentage|beacon_reorg_count|beacon_reorg_depth)|eth_exe_(block_most_recent_number|block_head_transactions_in_block|gas_price_gwei|sync_is_syncing|sync_percentage))\{[^}]*\}`)
	n := 0
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if m := consumed.FindString(sc.Text()); m != "" {
			n++
			if !strings.Contains(got, m+" ") {
				t.Errorf("exporter series %s not emitted with identical labels", m)
			}
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	if n != 12 {
		t.Errorf("matched %d sample series, want 12 (sample changed?)", n)
	}
}

func TestFailureKeepsValuesAndDropsHealth(t *testing.T) {
	f, p := setup(t)
	p.PollOnce(context.Background())
	f.mu.Lock()
	f.down = true
	f.mu.Unlock()
	p.PollOnce(context.Background())
	mustHave(t, scrape(t, p),
		`eth_con_health_up{module="health",node="consensus"} 0`,
		`eth_con_sync_head_slot{module="sync",node="consensus"} 158`,
		`eth_exe_health_up{ethereum_role="execution",module="health",node_name="execution"} 0`,
		`eth_exe_block_most_recent_number{ethereum_role="execution",identifier="head",module="block",node_name="execution"} 158`,
	)
}

// A hung (not refused) beacon used to keep health_up=1; the poll timeout
// now turns it into health_up=0 within one timeout.
func TestHungBeaconIsUnhealthy(t *testing.T) {
	f, p := setup(t)
	f.mu.Lock()
	f.hang = true
	f.mu.Unlock()
	start := time.Now()
	p.PollOnce(context.Background())
	if d := time.Since(start); d > time.Second {
		t.Errorf("poll took %s, want <= timeout", d)
	}
	mustHave(t, scrape(t, p), `eth_con_health_up{module="health",node="consensus"} 0`)
}

func TestExecutionSyncing(t *testing.T) {
	f, p := setup(t)
	f.mu.Lock()
	f.elSyncing = true
	f.mu.Unlock()
	p.PollOnce(context.Background())
	mustHave(t, scrape(t, p),
		`eth_exe_sync_is_syncing{ethereum_role="execution",module="sync",node_name="execution"} 1`,
		`eth_exe_sync_percentage{ethereum_role="execution",module="sync",node_name="execution"} 25`,
	)
}

func TestReorgStream(t *testing.T) {
	f, p := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { p.Run(ctx); close(done) }()
	f.reorgs <- "2"
	f.reorgs <- "3"
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(scrape(t, p), `eth_con_beacon_reorg_count{module="beacon",node="consensus"} 2`) {
		if time.Now().After(deadline) {
			t.Fatalf("reorgs not counted:\n%s", scrape(t, p))
		}
		time.Sleep(10 * time.Millisecond)
	}
	mustHave(t, scrape(t, p), `eth_con_beacon_reorg_depth{module="beacon",node="consensus"} 5`)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestRedirectNotFollowed(t *testing.T) {
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("redirect followed to %s", r.URL)
	}))
	defer other.Close()
	b := httptest.NewServer(http.RedirectHandler(other.URL+"/eth/v1/node/syncing", http.StatusFound))
	defer b.Close()
	p := New(Config{BeaconURL: b.URL})
	p.PollOnce(context.Background())
	mustHave(t, scrape(t, p), `eth_con_health_up{module="health",node="consensus"} 0`)
}

func TestDisabledSideNotExported(t *testing.T) {
	_, full := setup(t)
	p := New(Config{BeaconURL: full.cfg.BeaconURL})
	p.PollOnce(context.Background())
	if body := scrape(t, p); strings.Contains(body, "eth_exe_") {
		t.Errorf("execution series exported without an execution URL:\n%s", body)
	}
}
