package analytics

import "math"

type point struct {
	ts   int64
	p, v float64
	r    float64 // log return vs previous tick
	hasR bool
	side int8 // +1 buy, -1 sell, 0 unknown
}

type Stats struct {
	Symbol    string
	TS        int64   // trade time of the latest tick
	Price     float64
	VWAP      float64
	VolBps    float64 // std dev of returns, basis points
	Imbalance float64 // -1..+1
	N         int     // ticks in window
	Z         float64
	Alert     bool
}

type Window struct {
	span   int64 // window length in ms of TRADE time
	zLimit float64
	pts    []point

	// running sums — updated on add and evict, so each tick is O(1)
	sumPV, sumV, buyV, sellV float64
	sumR, sumR2              float64
	nR                       int

	lastP    float64
	lastSide int8
	maxTS    int64
	sampleTS int64   // trade time of the last 1-second sample
	sampleP  float64 // price at the last sample
}

func NewWindow(spanMs int64, zLimit float64) *Window {
	return &Window{span: spanMs, zLimit: zLimit}
}

func (w *Window) Add(symbol string, ts int64, p, v float64) Stats {
	pt := point{ts: ts, p: p, v: v, side: w.lastSide}

	// tick rule still uses EVERY trade (for order-flow imbalance)
	if w.lastP > 0 {
		if p > w.lastP {
			pt.side = 1
		} else if p < w.lastP {
			pt.side = -1
		}
	}

	// returns use 1-second samples to remove bid-ask bounce noise
	if w.sampleP == 0 {
		w.sampleTS, w.sampleP = ts, p
	} else if ts-w.sampleTS >= 1000 {
		pt.r, pt.hasR = math.Log(p/w.sampleP), true
		w.sampleTS, w.sampleP = ts, p
	}

	// z-score: compare the NEW return against PAST returns (before adding it)
	var z float64
	alert := false
	if pt.hasR && w.nR >= 30 {
		mean := w.sumR / float64(w.nR)
		variance := w.sumR2/float64(w.nR) - mean*mean
		if variance > 0 {
			z = (pt.r - mean) / math.Sqrt(variance)
			alert = math.Abs(z) >= w.zLimit
		}
	}

	// add
	w.pts = append(w.pts, pt)
	w.apply(pt, +1)
	w.lastP, w.lastSide = p, pt.side
	if ts > w.maxTS {
		w.maxTS = ts // a late tick doesn't move the window backwards
	}

	// evict ticks older than the window
	cutoff := w.maxTS - w.span
	i := 0
	for i < len(w.pts) && w.pts[i].ts < cutoff {
		w.apply(w.pts[i], -1)
		i++
	}
	w.pts = w.pts[i:]

	s := Stats{Symbol: symbol, TS: ts, Price: p, N: len(w.pts), Z: z, Alert: alert}
	if w.sumV > 0 {
		s.VWAP = w.sumPV / w.sumV
		s.Imbalance = (w.buyV - w.sellV) / w.sumV
	}
	if w.nR > 1 {
		mean := w.sumR / float64(w.nR)
		if variance := w.sumR2/float64(w.nR) - mean*mean; variance > 0 {
			s.VolBps = math.Sqrt(variance) * 1e4
		}
	}
	return s
}

// apply adds (sign=+1) or removes (sign=-1) one point's contribution.
func (w *Window) apply(pt point, sign float64) {
	w.sumPV += sign * pt.p * pt.v
	w.sumV += sign * pt.v
	switch pt.side {
	case 1:
		w.buyV += sign * pt.v
	case -1:
		w.sellV += sign * pt.v
	}
	if pt.hasR {
		w.sumR += sign * pt.r
		w.sumR2 += sign * pt.r * pt.r
		w.nR += int(sign)
	}
}