package main

import (
	"bufio"
	"context"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/blackheart-5/market-engine/internal/tick"
	"github.com/segmentio/kafka-go"
)

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func main() {
	rate, _ := strconv.Atoi(env("RATE", "0"))
	loops, _ := strconv.Atoi(env("LOOPS", "1"))

	// load the whole recording into memory so disk reads don't slow the replay
	f, err := os.Open(env("FILE", "ticks.jsonl"))
	if err != nil {
		log.Fatal(err)
	}
	var ticks []tick.Tick
	var minTS, maxTS int64
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var t tick.Tick
		if json.Unmarshal(sc.Bytes(), &t) != nil {
			continue
		}
		if minTS == 0 || t.TradeTS < minTS {
			minTS = t.TradeTS
		}
		if t.TradeTS > maxTS {
			maxTS = t.TradeTS
		}
		ticks = append(ticks, t)
	}
	f.Close()
	shift := maxTS - minTS + 120_000 // each loop starts 2 min after the previous one ends
	log.Printf("loaded %d ticks, replaying x%d at rate=%d (0=max)", len(ticks), loops, rate)

	w := &kafka.Writer{
		Addr:         kafka.TCP("localhost:19092"),
		Topic:        env("TOPIC", "ticks-replay"),
		Balancer:     &kafka.Hash{},
		BatchSize:    1000,
		BatchTimeout: 2 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
	}
	defer w.Close()

	batch := make([]kafka.Message, 0, 1000)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := w.WriteMessages(context.Background(), batch...); err != nil {
			log.Fatal(err)
		}
		batch = batch[:0]
	}

	start := time.Now()
	lastFlush := start
	sent := 0
	for l := 0; l < loops; l++ {
		for _, t := range ticks {
			t.TradeTS += int64(l) * shift
			t.IngestNS = time.Now().UnixNano() // latency now measures OUR pipeline
			t.Seq = uint64(sent)
			b, _ := json.Marshal(t)
			batch = append(batch, kafka.Message{Key: []byte(t.Symbol), Value: b})
			sent++

			// flush when full, or every 2ms so paced ticks don't sit waiting in a batch
			if len(batch) == cap(batch) || time.Since(lastFlush) >= 2*time.Millisecond {
				flush()
				lastFlush = time.Now()
			}
			if rate > 0 { // pacing: stay on schedule for `rate` ticks/sec
				target := start.Add(time.Duration(sent) * time.Second / time.Duration(rate))
				if d := time.Until(target); d > time.Millisecond {
					time.Sleep(d)
				}
			}
		}
	}
	flush()
	el := time.Since(start).Seconds()
	log.Printf("sent %d ticks in %.2fs = %.0f ticks/s", sent, el, float64(sent)/el)
}