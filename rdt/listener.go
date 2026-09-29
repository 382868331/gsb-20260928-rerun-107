package rdt

import (
	"context"
	"sync"
	"time"
)

// Listener accepts inbound dials on one local address. The per-epoch session
// is created at the first SYN, exactly like the dialer side, so data that
// arrives in the same fault burst as the final handshake ACK is never lost.
// Duplicated SYNs only retransmit the SYN-ACK.
type Listener struct {
	mux   *Mux
	local Addr
	cfg   Config

	mu      sync.Mutex
	pending map[muxKey]struct{}
	conns   map[muxKey]*Conn
	accept  chan *Conn
	closed  chan struct{}
	once    sync.Once
}

func Listen(mux *Mux, local Addr, cfg Config) *Listener {
	cfg = cfg.withDefaults()
	l := &Listener{
		mux:     mux,
		local:   local,
		cfg:     cfg,
		pending: make(map[muxKey]struct{}),
		conns:   make(map[muxKey]*Conn),
		accept:  make(chan *Conn, 16),
		closed:  make(chan struct{}),
	}
	mux.setListener(local, l)
	return l
}

func (l *Listener) Accept(ctx context.Context) (*Conn, error) {
	select {
	case c := <-l.accept:
		return c, nil
	case <-l.closed:
		return nil, ErrClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (l *Listener) Close() {
	l.once.Do(func() { close(l.closed) })
}

func (l *Listener) onFrame(f Frame) {
	p := f.Pkt
	key := muxKey{remote: f.From, epoch: p.Epoch}

	l.mu.Lock()
	conn := l.conns[key]
	l.mu.Unlock()

	if conn == nil {
		if !p.has(flagSYN) || p.has(flagACK) {
			return
		}
		l.startSession(f, key)
		return
	}
	if p.has(flagSYN) {
		l.sendSYNACK(key)
		return
	}
	conn.localFrame(f)
}

func (l *Listener) startSession(f Frame, key muxKey) {
	l.mu.Lock()
	if c, ok := l.conns[key]; ok {
		l.mu.Unlock()
		c.send(Packet{Seq: 0, Ack: 1, Flags: flagSYN | flagACK})
		return
	}
	btr := BoundTransport{T: l.mux.tr, Self: l.local}
	c := newConn(btr, key.remote, key.epoch, l.cfg, 1)
	l.conns[key] = c
	l.pending[key] = struct{}{}
	c.onClose = func() {
		l.mu.Lock()
		if cur := l.conns[key]; cur == c {
			delete(l.conns, key)
		}
		delete(l.pending, key)
		l.mu.Unlock()
	}
	l.mu.Unlock()
	c.markDataReady()
	go l.handshake(c, key)
}

func (l *Listener) handshake(c *Conn, key muxKey) {
	resend := time.NewTicker(l.cfg.HandshakeTO)
	defer resend.Stop()
	l.sendSYNACK(key)
	for {
		select {
		case <-l.closed:
			c.shutdown(true)
			return
		case <-c.closeSig:
			return
		case p := <-c.hskFrames:
			if p.has(flagRST) {
				c.shutdown(true)
				return
			}
			if p.has(flagACK) && p.Ack == 1 {
				l.completeHandshake(c, key)
				return
			}
		case <-resend.C:
			l.sendSYNACK(key)
		}
	}
}

func (l *Listener) completeHandshake(c *Conn, key muxKey) {
	l.mu.Lock()
	delete(l.pending, key)
	l.mu.Unlock()
	c.markEstablished()
	select {
	case l.accept <- c:
	case <-l.closed:
		c.shutdown(true)
	}
}

func (l *Listener) sendSYNACK(key muxKey) {
	p := Packet{Epoch: key.epoch, Seq: 0, Ack: 1, Flags: flagSYN | flagACK}
	l.mux.tr.Send(Frame{From: l.local, To: key.remote, Pkt: p})
}
