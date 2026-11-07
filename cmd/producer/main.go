package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/blackheart-5/market-engine/internal/tick"
	"github.com/segmentio/kafka-go"
)

// Shape of Finnhub's messages: {"type":"trade","data":[{s,p,t,v}, ...]}
type finnhubMsg struct {
	Type string `json:"type"`
	Data []struct {
		S string  `json:"s"`
		P float64 `json:"p"`
		T int64   `json:"t"`
		V float64 `json:"v"`
	} `json:"data"`
}

// NEW: counters shared between goroutines. atomic = safe without locks.
var received, published, dropped atomic.Uint64

func brokers() string {
	if v := os.Getenv("KAFKA_BROKERS"); v != "" {
		return v
	}
	return "localhost:19092"
}

func main() {
	token := os.Getenv("FINNHUB_TOKEN")
	if token == "" {
		log.Fatal("set FINNHUB_TOKEN")
	}

	// Kafka writer
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers()),
		Topic:        "ticks",
		Balancer:     &kafka.Hash{},      // hash(key) -> partition, so each symbol stays on one partition
		BatchTimeout: 5 * time.Millisecond, // send a batch at least every 5ms
		RequiredAcks: kafka.RequireOne,     // broker confirms the write
	}
	defer w.Close()

	out := make(chan kafka.Message, 50_000) // channel == the shock absorber
	go writeLoop(w, out)                    //
	go statsLoop()                          //

	conn, _, err := websocket.DefaultDialer.Dial("wss://ws.finnhub.io?token="+token, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// 20 symbols instead of 3
	symbols := []string{
		"BINANCE:BTCUSDT", "BINANCE:ETHUSDT", "BINANCE:SOLUSDT", "BINANCE:BNBUSDT", "BINANCE:XRPUSDT",
		"BINANCE:DOGEUSDT", "BINANCE:ADAUSDT", "BINANCE:AVAXUSDT", "BINANCE:LINKUSDT", "BINANCE:LTCUSDT",
		"BINANCE:DOTUSDT", "BINANCE:TRXUSDT", "BINANCE:BCHUSDT", "BINANCE:NEARUSDT", "BINANCE:ATOMUSDT",
		"BINANCE:UNIUSDT", "BINANCE:ETCUSDT", "BINANCE:FILUSDT", "BINANCE:APTUSDT", "BINANCE:ARBUSDT",
	}
	for _, s := range symbols {
		conn.WriteJSON(map[string]string{"type": "subscribe", "symbol": s})
	}
	log.Printf("subscribed to %d symbols", len(symbols))

	var seq uint64
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			log.Fatal(err)
		}
		now := time.Now().UnixNano()

		var m finnhubMsg
		if json.Unmarshal(raw, &m) != nil || m.Type != "trade" {
			continue
		}
		for _, d := range m.Data {
			seq++
			received.Add(1)
			t := tick.Tick{Symbol: d.S, Price: d.P, Volume: d.V, TradeTS: d.T, IngestNS: now, Seq: seq}
			b, _ := json.Marshal(t)

			// NEW: non-blocking send — this IS the backpressure policy
			select {
			case out <- kafka.Message{Key: []byte(d.S), Value: b}:
			default:
				dropped.Add(1) // channel full: drop + count, never stall the socket
			}
		}
	}
}

// NEW: pulls ticks off the channel and writes them to Kafka in batches
func writeLoop(w *kafka.Writer, in <-chan kafka.Message) {
	batch := make([]kafka.Message, 0, 500)
	ticker := time.NewTicker(5 * time.Millisecond)
	for {
		select {
		case m := <-in:
			batch = append(batch, m)
			if len(batch) < 500 {
				continue // keep filling until full or the ticker fires
			}
		case <-ticker.C:
			if len(batch) == 0 {
				continue
			}
		}
		if err := w.WriteMessages(context.Background(), batch...); err != nil {
			log.Printf("kafka error: %v", err)
			dropped.Add(uint64(len(batch)))
		} else {
			published.Add(uint64(len(batch)))
		}
		batch = batch[:0]
	}
}

// NEW: prints a health line every 5 seconds instead of every tick
func statsLoop() {
	var last uint64
	for range time.Tick(5 * time.Second) {
		p := published.Load()
		log.Printf("received=%d published=%d dropped=%d rate=%.1f ticks/s",
			received.Load(), p, dropped.Load(), float64(p-last)/5)
		last = p
	}
}