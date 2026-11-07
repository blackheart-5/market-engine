package tick

type Tick struct {
	Symbol   string  `json:"s"`
	Price    float64 `json:"p"`
	Volume   float64 `json:"v"`
	TradeTS  int64   `json:"t"`         // exchange trade time (ms)
	IngestNS int64   `json:"ingest_ns"` // when WE received it (ns)
	Seq      uint64  `json:"seq"`       // our sequence number
}