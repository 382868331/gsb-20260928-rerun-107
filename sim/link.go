// Package sim provides deterministic, replayable unreliable transports:
// loss and delay are decided by a seeded script, so a failing scenario
// can be reproduced exactly. Faults stay active for the whole run.
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
	// MinDelay/MaxDelay bound the uniform per-frame delivery delay.
	MinDelay time.Duration
	MaxDelay time.Duration
}

// pair models the wire. Each direction has:
//   - in:  frames handed to the wire (Send)
//   - out: frames due for delivery (Recv)
//
// A pumper per direction draws loss decisions in strict arrival order
// (reproducible for a fixed seed); the delay sleep runs per-frame so a
// slow frame never head-of-line blocks the others.
type pair struct {
	closed chan struct{}
	once   sync.Once
	wg     sync.WaitGroup
}

type direction struct {
	cfg Config
	rng *rand.Rand
	in  chan rdt.Frame
	out chan rdt.Frame
}

// Link implements rdt.Transport.
type Link struct {
	p      *pair
	sendCh chan rdt.Frame
	recvCh chan rdt.Frame
}

var errClosed = errors.New("sim: link closed")

// NewPair builds a bidirectional faulty link pair from one seed.
func NewPair(seed int64, ab, ba Config) Pair {
	p := &pair{closed: make(chan struct{})}
	aToB := &direction{
		cfg: ba,
		rng: rand.New(rand.NewSource(seed*2 + 1)),
		in:  make(chan rdt.Frame, 1<<16),
		out: make(chan rdt.Frame, 1<<16),
	}
	bToA := &direction{
		cfg: ab,
		rng: rand.New(rand.NewSource(seed*2 + 2)),
		in:  make(chan rdt.Frame, 1<<16),
		out: make(chan rdt.Frame, 1<<16),
	}
	p.wg.Add(2)
	go pump(aToB, p.closed, &p.wg)
	go pump(bToA, p.closed, &p.wg)
	a := &Link{p: p, sendCh: aToB.in, recvCh: bToA.out}
	b := &Link{p: p, sendCh: bToA.in, recvCh: aToB.out}
	return Pair{A: a, B: b}
}

func pump(d *direction, closed <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case f := <-d.in:
			if d.cfg.LossRate > 0 && d.rng.Float64() < d.cfg.LossRate {
				continue // simulated loss, fault process still running
			}
			wait := d.cfg.MinDelay
			if d.cfg.MaxDelay > d.cfg.MinDelay {
				wait += time.Duration(d.rng.Int63n(int64(d.cfg.MaxDelay-d.cfg.MinDelay) + 1))
			}
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
		case <-closed:
			return
		}
	}
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
