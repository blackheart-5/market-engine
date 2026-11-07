package main

import (
	"context"
	"encoding/json"
	"fmt"
	"hash"
	"hash/fnv"
	"log"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/blackheart-5/market-engine/internal/analytics"
	"github.com/blackheart-5/market-engine/internal/tick"
	"github.com/segmentio/kafka-go"
)

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

type dashboard struct {
	mu       sync.Mutex
	latest   map[string]analytics.Stats
	alerts   []string
	p50, p99 float64
	tps      float64
}

func (d *dashboard) serveStats(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	defer d.mu.Unlock()
	rows := make([]analytics.Stats, 0, len(d.latest))
	for _, s := range d.latest {
		rows = append(rows, s)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Symbol < rows[j].Symbol })
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"rows": rows, "alerts": d.alerts, "p50": d.p50, "p99": d.p99, "tps": d.tps,
	})
}

// digest combines per-symbol hashes in sorted order -> independent of cross-partition interleaving
func digest(hs map[string]hash.Hash64) string {
	syms := make([]string, 0, len(hs))
	for s := range hs {
		syms = append(syms, s)
	}
	sort.Strings(syms)
	f := fnv.New64a()
	for _, s := range syms {
		fmt.Fprintf(f, "%s:%x;", s, hs[s].Sum64())
	}
	return fmt.Sprintf("%016x", f.Sum64())
}

func pct(sorted []float64, q float64) float64 {
	return sorted[int(q*float64(len(sorted)-1))]
}

func main() {
	topic, group := env("TOPIC", "ticks"), env("GROUP", "analytics")
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{env("KAFKA_BROKERS", "localhost:19092")},
		Topic:          topic,
		GroupID:        group,
		StartOffset:    kafka.FirstOffset,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        10 * time.Millisecond,
		CommitInterval: time.Second,
	})
	defer r.Close()

	dash := &dashboard{latest: map[string]analytics.Stats{}}
	http.HandleFunc("/stats", dash.serveStats)
	http.Handle("/", http.FileServer(http.Dir("web")))
	go func() {
		port := env("PORT", "8090")
		log.Println("dashboard on http://localhost:" + port)
		log.Fatal(http.ListenAndServe(":"+port, nil))
	}()

	msgs := make(chan kafka.Message, 10_000)
	go func() {
		for {
			m, err := r.ReadMessage(context.Background())
			if err != nil {
				log.Fatal(err)
			}
			msgs <- m
		}
	}()

	windows := map[string]*analytics.Window{}
	hashes := map[string]hash.Hash64{}
	var lat, runLat []float64
	var runStart, runLast time.Time
	runAlerts := 0
	report := time.NewTicker(5 * time.Second)
	log.Printf("consumer started topic=%s group=%s", topic, group)

	for {
		select {
		case m := <-msgs:
			var t tick.Tick
			if json.Unmarshal(m.Value, &t) != nil {
				continue
			}
			w, ok := windows[t.Symbol]
			if !ok {
				w = analytics.NewWindow(60_000, 4.0)
				windows[t.Symbol] = w
			}
			s := w.Add(t.Symbol, t.TradeTS, t.Price, t.Volume)

			// determinism fingerprint: every output this symbol produces, in order
			h, ok := hashes[t.Symbol]
			if !ok {
				h = fnv.New64a()
				hashes[t.Symbol] = h
			}
			fmt.Fprintf(h, "%d|%.10g|%.10g|%.10g|%t;", s.TS, s.VWAP, s.VolBps, s.Imbalance, s.Alert)

			dash.mu.Lock()
			dash.latest[t.Symbol] = s
			if s.Alert {
				runAlerts++
				a := fmt.Sprintf("%s  %s  z=%+.1f  px=%.4f",
					time.UnixMilli(s.TS).Format("15:04:05"), s.Symbol, s.Z, s.Price)
				dash.alerts = append([]string{a}, dash.alerts...)
				if len(dash.alerts) > 20 {
					dash.alerts = dash.alerts[:20]
				}
			}
			dash.mu.Unlock()

			now := time.Now()
			ms := float64(now.UnixNano()-t.IngestNS) / 1e6
			lat = append(lat, ms)
			runLat = append(runLat, ms)
			if runStart.IsZero() {
				runStart = now
			}
			runLast = now

		case <-report.C:
			if len(lat) == 0 {
				dash.mu.Lock()
				dash.p50, dash.p99, dash.tps = 0, 0, 0
				dash.mu.Unlock()
				// ticks stopped -> print the summary for the run that just ended
				if len(runLat) > 0 {
					sort.Float64s(runLat)
					el := runLast.Sub(runStart).Seconds()
					log.Printf("=== RUN SUMMARY: ticks=%d symbols=%d time=%.2fs throughput=%.0f ticks/s p50=%.1fms p99=%.1fms alerts=%d digest=%s",
						len(runLat), len(hashes), el, float64(len(runLat))/el,
						pct(runLat, 0.50), pct(runLat, 0.99), runAlerts, digest(hashes))
					runLat, runStart, runAlerts = nil, time.Time{}, 0
					hashes = map[string]hash.Hash64{}
					windows = map[string]*analytics.Window{} // fresh state for the next run
				}
				continue
			}
			sort.Float64s(lat)
			log.Printf("ticks=%d  p50=%.1fms  p99=%.1fms  max=%.1fms",
				len(lat), pct(lat, 0.50), pct(lat, 0.99), lat[len(lat)-1])
			dash.mu.Lock()
			dash.p50, dash.p99, dash.tps = pct(lat, 0.50), pct(lat, 0.99), float64(len(lat))/5
			dash.mu.Unlock()
			lat = lat[:0]
		}
	}
}