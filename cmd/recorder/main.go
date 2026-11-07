package main

import (
	"bufio"
	"context"
	"log"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{"localhost:19092"},
		Topic:       "ticks",
		GroupID:     "recorder-" + time.Now().Format("150405"), // new group = read from the start
		StartOffset: kafka.FirstOffset,
		MinBytes:    1,
		MaxBytes:    10e6,
		MaxWait:     100 * time.Millisecond,
	})
	defer r.Close()

	f, err := os.Create("ticks.jsonl")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	bw := bufio.NewWriter(f)
	defer bw.Flush()

	n := 0
	timeout := 15 * time.Second // first fetch waits for the consumer group to join
	for {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		m, err := r.FetchMessage(ctx) // Fetch (not Read) = don't commit; we're just tapping
		cancel()
		if err != nil {
			break // timeout = caught up
		}
		bw.Write(m.Value)
		bw.WriteByte('\n')
		n++
		timeout = 3 * time.Second
		if n%10000 == 0 {
			log.Printf("recorded %d", n)
		}
	}
	log.Printf("done: %d ticks -> ticks.jsonl", n)
}