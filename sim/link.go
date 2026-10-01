// Package sim provides deterministic, replayable unreliable transports:
// loss, duplication and delay are decided by a seeded script, so a
// failing scenario can be reproduced exactly. Faults stay active for the
// whole run; the decision script can be captured and replayed.
package sim

import (
	"errors"
	"math/rand"
	"sync"
	"time"

	"rdt/rdt"
)

// Pair is two interconnected transports. Closing either side breaks both.
type Pair struct {
	A *Link
	B *Link
}

// Config describes the fault process for one direction of a link.
type Config struct {
	// LossRate is applied independently per frame, including acks and syn.
	LossRate float64
	// DupRate is the probability that a delivered frame is duplicated.
	// The duplicate travels with its own random delay, so duplicates can
	// overtake the original (or other traffic), creating repeats/reordering.
	DupRate float64
	// MinDelay/MaxDelay bound the uniform per-frame delivery delay.
	MinDelay time.Duration
	MaxDelay time.Duration
}

// Decision is the recorded outcome of the seeded script for one frame.
// A captured sequence replays byte-for-byte identical network behavior.
type Decision struct {
	Direction string // "A->B" or "B->A"
	Index     int    // frame ordinal in that direction
	Lost      bool
	Dups      int // extra copies delivered
	Delay     time.Duration
	DupDelays []time.Duration
}

// Journal collects every fault decision for later replay.
type Journal struct {
	mu        sync.Mutex
	Decisions []Decision
}

func (j *Journal) record(d Decision) {
	if j == nil {
		return
	}
	j.mu.Lock()
	j.Decisions = append(j.Decisions, d)
	j.mu.Unlock()
}

// Snapshot returns a copy of the recorded decisions.
func (j *Journal) Snapshot() []Decision {
	if j == nil {
		return nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make([]Decision, len(j.Decisions))
	copy(out, j.Decisions)
	return out
}

// Replay replays previously recorded decisions instead of drawing new
// random ones. Nil restores seeded randomness.
type Replay struct {
	mu     sync.Mutex
	script []Decision
	pos    map[string]int
}

// NewReplay builds a deterministic replay source from a Journal snapshot.
func NewReplay(decisions []Decision) *Replay {
	return &Replay{script: decisions, pos: map[string]int{}}
}

func (r *Replay) next(dir string, idx int) (Decision, bool) {
	if r == nil {
		return Decision{}, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for p := r.pos[dir]; p < len(r.script); p++ {
		if r.script[p].Direction == dir && r.script[p].Index == idx {
			r.pos[dir] = p + 1
			return r.script[p], true
		}
	}
	return Decision{}, false
}

type pair struct {
	closed chan struct{}
	once   sync.Once
	wg     sync.WaitGroup
}

type direction struct {
	tag     string
	cfg     Config
	rng     *rand.Rand
	index   int
	in      chan rdt.Frame
	out     chan rdt.Frame
	journal *Journal
	replay  *Replay
}

// Link implements rdt.Transport.
type Link struct {
	p      *pair
	sendCh chan rdt.Frame
	recvCh chan rdt.Frame
}

var errClosed = errors.New("sim: link closed")

// Options controls a pair's fault recording/replay.
type Options struct {
	Journal *Journal
	Replay  *Replay
}

// NewPair builds a bidirectional faulty link pair from one seed.
func NewPair(seed int64, ab, ba Config) Pair {
	return NewPairOptions(seed, ab, ba, Options{})
}

// NewPairOptions builds a pair that records decisions to Journal and/or
// follows a fixed Replay script instead of the seeded RNG.
func NewPairOptions(seed int64, ab, ba Config, opts Options) Pair {
	return newPairImpl(seed, ab, ba, opts)
}

func newPairImpl(seed int64, ab, ba Config, opts Options) Pair {
	p := &pair{closed: make(chan struct{})}
	aToB := &direction{
		tag: "A->B", cfg: ab, rng: rand.New(rand.NewSource(seed*2 + 1)),
		in: make(chan rdt.Frame, 1<<16), out: make(chan rdt.Frame, 1<<16),
		journal: opts.Journal, replay: opts.Replay,
	}
	bToA := &direction{
		tag: "B->A", cfg: ba, rng: rand.New(rand.NewSource(seed*2 + 2)),
		in: make(chan rdt.Frame, 1<<16), out: make(chan rdt.Frame, 1<<16),
		journal: opts.Journal, replay: opts.Replay,
	}
	p.wg.Add(2)
	go pump(aToB, p.closed, &p.wg)
	go pump(bToA, p.closed, &p.wg)
	a := &Link{p: p, sendCh: aToB.in, recvCh: bToA.out}
	b := &Link{p: p, sendCh: bToA.in, recvCh: aToB.out}
	return Pair{A: a, B: b}
}

func (d *direction) decide() Decision {
	if dec, ok := d.replay.next(d.tag, d.index); ok {
		return dec
	}
	dec := Decision{Direction: d.tag, Index: d.index}
	if d.cfg.LossRate > 0 && d.rng.Float64() < d.cfg.LossRate {
		dec.Lost = true
	}
	dec.Delay = d.cfg.MinDelay
	if d.cfg.MaxDelay > d.cfg.MinDelay {
		dec.Delay += time.Duration(d.rng.Int63n(int64(d.cfg.MaxDelay-d.cfg.MinDelay) + 1))
	}
	if d.cfg.DupRate > 0 && d.rng.Float64() < d.cfg.DupRate {
		dec.Dups = 1
		w := d.cfg.MinDelay
		if d.cfg.MaxDelay > d.cfg.MinDelay {
			w += time.Duration(d.rng.Int63n(int64(d.cfg.MaxDelay-d.cfg.MinDelay) + 1))
		}
		dec.DupDelays = []time.Duration{w}
	}
	d.journal.record(dec)
	return dec
}

func pump(d *direction, closed <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case f := <-d.in:
			dec := d.decide()
			d.index++
			if dec.Lost {
				continue
			}
			deliver(d, f, dec.Delay, closed)
			for k := 0; k < dec.Dups; k++ {
				w := dec.Delay
				if k < len(dec.DupDelays) {
					w = dec.DupDelays[k]
				}
				deliver(d, f, w, closed)
			}
		case <-closed:
			return
		}
	}
}

func deliver(d *direction, f rdt.Frame, wait time.Duration, closed <-chan struct{}) {
	go func(f rdt.Frame, wait time.Duration) {
		if wait > 0 {
			t := time.NewTimer(wait)
			defer t.Stop()
			select {
			case <-t.C:
			case <-closed:
				return
			}
		}
		select {
		case d.out <- f:
		case <-closed:
		default:
		}
	}(f, wait)
}

// Send applies the fault script and hands the frame to the wire.
func (l *Link) Send(f rdt.Frame) error {
	select {
	case l.sendCh <- f:
	case <-l.p.closed:
		return errClosed
	default:
		// Saturated wire: behave like a dropped datagram. Never block a
		// caller that may hold protocol locks.
	}
	return nil
}

// Recv blocks until a frame is due for delivery.
func (l *Link) Recv() (rdt.Frame, error) {
	select {
	case f := <-l.recvCh:
		return f, nil
	case <-l.p.closed:
		return rdt.Frame{}, errClosed
	}
}

// Close tears down both directions and unblocks every Send/Recv.
func (l *Link) Close() error {
	l.p.once.Do(func() { close(l.p.closed) })
	return nil
}
