package rdt

import (
	"context"
	"sync"
	"time"
)

// Mux owns one receive pump per local address and demuxes frames to
// established sessions or pending handshakes. The epoch chosen by the dialer
// names the session on both ends, so a rebuilt dial gets a new epoch and stale
// frames from the old session cannot reach the new session.
type Mux struct {
	tr *FaultyTransport

	mu    sync.Mutex
	conns map[muxKey]*Conn
	lis   map[Addr]*Listener
}

type muxKey struct {
	remote Addr
	epoch  uint64
}

// NewMux starts receive pumps for the given local addresses.
func NewMux(tr *FaultyTransport, locals ...Addr) *Mux {
	m := &Mux{
		tr:    tr,
		conns: make(map[muxKey]*Conn),
		lis:   make(map[Addr]*Listener),
	}
	for _, a := range locals {
		tr.Register(a, 2048)
		go m.pump(a)
	}
	return m
}

func (m *Mux) pump(local Addr) {
	for {
		f, ok := m.tr.RecvFor(local)
		if !ok {
			return
		}
		m.dispatch(f)
	}
}

func (m *Mux) dispatch(f Frame) {
	p := f.Pkt
	key := muxKey{remote: f.From, epoch: p.Epoch}
	m.mu.Lock()
	conn := m.conns[key]
	lis := m.lis[f.To]
	m.mu.Unlock()
	if conn != nil {
		conn.localFrame(f)
		return
	}
	if lis != nil {
		lis.onFrame(f)
		return
	}
	// Unknown session: nothing to do. A later SYN with a new epoch creates a
	// fresh session; this frame is simply dropped, never guessed into one.
}

func (m *Mux) setListener(local Addr, l *Listener) {
	m.mu.Lock()
	m.lis[local] = l
	m.mu.Unlock()
}

func (m *Mux) registerConn(remote Addr, epoch uint64, c *Conn) {
	m.mu.Lock()
	key := muxKey{remote: remote, epoch: epoch}
	m.conns[key] = c
	c.onClose = func() {
		m.mu.Lock()
		if cur := m.conns[key]; cur == c {
			delete(m.conns, key)
		}
		m.mu.Unlock()
	}
	m.mu.Unlock()
}

// Dial establishes a session to remote. The dialer's freshly generated epoch
// identifies the connection; retransmitted SYNs reuse it while each new Dial
// call uses a different one.
func (m *Mux) Dial(ctx context.Context, local, remote Addr, cfg Config) (*Conn, error) {
	cfg = cfg.withDefaults()
	epoch := newEpoch()

	btr := BoundTransport{T: m.tr, Self: local}
	c := newConn(btr, remote, epoch, cfg, 1)
	m.registerConn(remote, epoch, c)
	connected := false
	defer func() {
		if !connected {
			c.send(Packet{Flags: flagRST})
			c.shutdown(true)
		}
	}()

	timer := time.NewTimer(0)
	defer timer.Stop()
	attempts := 0
	for {
		c.send(Packet{Seq: 0, Flags: flagSYN})
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(cfg.HandshakeTO)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case p := <-c.hskFrames:
			if p.has(flagRST) {
				return nil, ErrRejected
			}
			if p.has(flagSYN) && p.has(flagACK) {
				c.send(Packet{Seq: 1, Ack: 1, Flags: flagACK})
				connected = true
				c.markEstablished()
				return c, nil
			}
		case <-timer.C:
			attempts++
			if attempts > 100 {
				return nil, ErrClosed
			}
		}
	}
}
