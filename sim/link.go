// Package sim provides deterministic, replayable unreliable transports.
package sim

import (
	"errors"
	"math/rand"
	"sync"
	"time"

	"rdt/rdt"
)

type Pair struct {
	A *Link
	B *Link
}

type Config struct {
	LossRate      float64
	DuplicateRate float64
	MinDelay      time.Duration
	MaxDelay      time.Duration
}

type Decision struct {
	Direction  string
	Index      uint64
	Kind       uint8
	Stream     uint32
	Seq        uint64
	Ack        uint64
	Delivered  bool
	Duplicated bool
	Delay      time.Duration
}

type pair struct {
	closed chan struct{}
	once   sync.Once
}

type direction struct {
	name string
	cfg  Config
	rng  *rand.Rand
	in   chan rdt.Frame
	out  chan rdt.Frame

	mu        sync.Mutex
	decisions []Decision
}

type Link struct {
	p      *pair
	send   *direction
	recv   *direction
	sendCh chan rdt.Frame
	recvCh chan rdt.Frame
}

var errClosed = errors.New("sim: link closed")

func NewPair(seed int64, ab, ba Config) Pair {
	p := &pair{closed: make(chan struct{})}
	aToB := &direction{
		name: "A->B",
		cfg:  ba,
		rng:  rand.New(rand.NewSource(seed*2 + 1)),
		in:   make(chan rdt.Frame, 1<<16),
		out:  make(chan rdt.Frame, 1<<16),
	}
	bToA := &direction{
		name: "B->A",
		cfg:  ab,
		rng:  rand.New(rand.NewSource(seed*2 + 2)),
		in:   make(chan rdt.Frame, 1<<16),
		out:  make(chan rdt.Frame, 1<<16),
	}
	go pump(aToB, p.closed)
	go pump(bToA, p.closed)
	a := &Link{p: p, send: aToB, recv: bToA, sendCh: aToB.in, recvCh: bToA.out}
	b := &Link{p: p, send: bToA, recv: aToB, sendCh: bToA.in, recvCh: aToB.out}
	return Pair{A: a, B: b}
}

func pump(d *direction, closed <-chan struct{}) {
	var index uint64
	for {
		select {
		case f := <-d.in:
			dec := Decision{Direction: d.name, Index: index, Kind: uint8(f.Kind), Stream: f.Stream, Seq: f.Seq, Ack: f.Ack}
			index++
			if d.cfg.LossRate > 0 && d.rng.Float64() < d.cfg.LossRate {
				d.record(dec)
				continue
			}
			dec.Delivered = true
			wait := d.cfg.MinDelay
			if d.cfg.MaxDelay > d.cfg.MinDelay {
				wait += time.Duration(d.rng.Int63n(int64(d.cfg.MaxDelay-d.cfg.MinDelay) + 1))
			}
			dec.Delay = wait
			if d.cfg.DuplicateRate > 0 && d.rng.Float64() < d.cfg.DuplicateRate {
				dec.Duplicated = true
			}
			d.record(dec)
			go deliver(d, f, wait, closed)
			if dec.Duplicated {
				go deliver(d, f, wait, closed)
			}
		case <-closed:
			return
		}
	}
}

func deliver(d *direction, f rdt.Frame, wait time.Duration, closed <-chan struct{}) {
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
}

func (d *direction) record(dec Decision) {
	d.mu.Lock()
	d.decisions = append(d.decisions, dec)
	d.mu.Unlock()
}

func (l *Link) Decisions() []Decision {
	l.send.mu.Lock()
	out := append([]Decision(nil), l.send.decisions...)
	l.send.mu.Unlock()
	return out
}

func (l *Link) Send(f rdt.Frame) error {
	select {
	case l.sendCh <- f:
	case <-l.p.closed:
		return errClosed
	default:
	}
	return nil
}

func (l *Link) Recv() (rdt.Frame, error) {
	select {
	case f := <-l.recvCh:
		return f, nil
	case <-l.p.closed:
		return rdt.Frame{}, errClosed
	}
}

func (l *Link) Close() error {
	l.p.once.Do(func() { close(l.p.closed) })
	return nil
}
