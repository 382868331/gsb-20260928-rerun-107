package rdt

import (
	"math/rand/v2"
	"sync"
	"time"
)

// Addr is a transport-level endpoint identifier.
type Addr string

// Frame is a packet together with its source and destination.
type Frame struct {
	From, To Addr
	Pkt      Packet
}

// Transport is the unreliable substrate the protocol runs on. Implementations
// may delay, reorder, duplicate or drop frames, but must never corrupt a frame
// into a different valid frame for the tests below.
type Transport interface {
	Send(f Frame)
	Recv() (Frame, bool)
	Close() error
}

// FaultProfile deterministically decides what happens to each frame attempt.
// The same seed always produces the same sequence of decisions, which lets a
// test replay a run exactly. Loss is active for the entire run; correctness is
// never achieved by switching faults off.
type FaultProfile struct {
	Seed              int64
	LossProbability   float64 // probability any given frame attempt is dropped
	DuplicateProb     float64 // probability an accepted frame is duplicated
	ReorderProb       float64 // probability a frame is pushed behind the next one
	MinDelay          time.Duration
	MaxDelay          time.Duration
	MaxInflightFrames int
	mu                sync.Mutex
	rng               *rand.Rand
}

// NewFaultProfile builds a profile. Loss defaults to a nonzero value so that a
// caller cannot accidentally run a "faulty" channel that never loses anything.
func NewFaultProfile(seed int64) *FaultProfile {
	return &FaultProfile{
		Seed:              seed,
		LossProbability:   0.3,
		DuplicateProb:     0.05,
		ReorderProb:       0.1,
		MinDelay:          500 * time.Microsecond,
		MaxDelay:          4 * time.Millisecond,
		MaxInflightFrames: 4096,
		rng:               rand.New(rand.NewPCG(uint64(seed), uint64(seed)^0x9e3779b97f4a7c15)),
	}
}

// FaultyTransport is an in-memory Transport with bounded buffering and
// deterministic, seedable loss/duplication/reordering/delay.
type FaultyTransport struct {
	prof  *FaultProfile
	in    chan Frame
	old   sync.Mutex
	conds map[Addr]chan Frame
	quit  chan struct{}
	wg    sync.WaitGroup
	once  sync.Once
}

// NewFaultyTransport wires two (or more) endpoints together. Every delivered
// frame is pushed to a per-destination channel of the given buffer size.
func NewFaultyTransport(prof *FaultProfile) *FaultyTransport {
	t := &FaultyTransport{
		prof:  prof,
		in:    make(chan Frame, prof.MaxInflightFrames),
		conds: make(map[Addr]chan Frame),
		quit:  make(chan struct{}),
	}
	t.wg.Add(1)
	go t.pump()
	return t
}

// Register allocates the receive mailbox for an address.
func (t *FaultyTransport) Register(a Addr, recvBuffer int) {
	t.old.Lock()
	defer t.old.Unlock()
	if _, ok := t.conds[a]; !ok {
		t.conds[a] = make(chan Frame, recvBuffer)
	}
}

func (t *FaultyTransport) mailbox(a Addr) chan Frame {
	t.old.Lock()
	defer t.old.Unlock()
	return t.conds[a]
}

// Send hands a frame to the fault-injection pump. The send itself never blocks
// on receiver readiness; backpressure comes from the bounded inflight channel.
func (t *FaultyTransport) Send(f Frame) {
	select {
	case t.in <- f:
	case <-t.quit:
	}
}

// RecvFor waits for a frame addressed to a.
func (t *FaultyTransport) RecvFor(a Addr) (Frame, bool) {
	c := t.mailbox(a)
	if c == nil {
		return Frame{}, false
	}
	select {
	case f := <-c:
		return f, true
	case <-t.quit:
		return Frame{}, false
	}
}

// Recv implements Transport for a single fixed endpoint; tests generally use
// RecvFor, and sessions construct a bound view via BoundTransport.
func (t *FaultyTransport) Recv() (Frame, bool) { return Frame{}, false }

func (t *FaultyTransport) pump() {
	defer t.wg.Done()
	pending := make([]scheduledFrame, 0, 64)
	var reorderHeld *scheduledFrame
	for {
		var timer <-chan time.Time
		var timerC *time.Timer
		var nextAt *time.Time
		if len(pending) > 0 {
			t := pending[0].at
			nextAt = &t
		}
		if reorderHeld != nil && (nextAt == nil || reorderHeld.at.Before(*nextAt)) {
			t := reorderHeld.at
			nextAt = &t
		}
		if nextAt != nil {
			d := time.Until(*nextAt)
			if d < 0 {
				d = 0
			}
			timerC = time.NewTimer(d)
			timer = timerC.C
		}
		select {
		case <-t.quit:
			if timerC != nil {
				timerC.Stop()
			}
			return
		case f := <-t.in:
			if timerC != nil {
				timerC.Stop()
			}
			t.decide(f, &pending, &reorderHeld)
		case <-timer:
			t.due(&pending, &reorderHeld)
		}
	}
}

type scheduledFrame struct {
	at time.Time
	f  Frame
}

func (t *FaultyTransport) decide(f Frame, pending *[]scheduledFrame, held **scheduledFrame) {
	p := t.prof
	p.mu.Lock()
	drop := p.rng.Float64() < p.LossProbability
	dup := !drop && p.rng.Float64() < p.DuplicateProb
	reorder := p.rng.Float64() < p.ReorderProb
	span := float64(p.MaxDelay - p.MinDelay)
	d := p.MinDelay + time.Duration(p.rng.Float64()*span)
	p.mu.Unlock()
	if drop {
		return
	}
	schedule := func(fr Frame, extra time.Duration) {
		sf := scheduledFrame{at: time.Now().Add(d + extra), f: fr}
		if reorder && *held == nil {
			*held = &sf
			return
		}
		if *held != nil {
			h := *held
			*held = nil
			insertSF(pending, *h)
		}
		insertSF(pending, sf)
	}
	schedule(f, 0)
	if dup {
		schedule(f, time.Millisecond)
	}
}

func insertSF(q *[]scheduledFrame, sf scheduledFrame) {
	i := len(*q)
	*q = append(*q, sf)
	for i > 0 && (*q)[i-1].at.After(sf.at) {
		(*q)[i] = (*q)[i-1]
		i--
	}
	(*q)[i] = sf
}

func (t *FaultyTransport) due(pending *[]scheduledFrame, held **scheduledFrame) {
	now := time.Now()
	for len(*pending) > 0 && !(*pending)[0].at.After(now) {
		sf := (*pending)[0]
		*pending = (*pending)[1:]
		c := t.mailbox(sf.f.To)
		if c != nil {
			select {
			case c <- sf.f:
			case <-t.quit:
				return
			}
		}
	}
	if len(*pending) == 0 && *held != nil && !(*held).at.After(now) {
		h := *held
		*held = nil
		insertSF(pending, *h)
	}
}

// Close releases pump goroutines and unblocks all receivers.
func (t *FaultyTransport) Close() error {
	t.once.Do(func() { close(t.quit) })
	t.wg.Wait()
	return nil
}

// BoundTransport is a Transport view fixed to one local address.
type BoundTransport struct {
	T    *FaultyTransport
	Self Addr
}

func (b BoundTransport) Send(f Frame)        { f.From = b.Self; b.T.Send(f) }
func (b BoundTransport) Recv() (Frame, bool) { return b.T.RecvFor(b.Self) }
func (b BoundTransport) Close() error        { return nil }
