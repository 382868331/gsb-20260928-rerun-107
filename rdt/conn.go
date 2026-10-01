package rdt

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"sync"
	"time"
)

// Stream-level errors.
var (
	// ErrStreamClosed is returned by Read/Write after a stream is fully closed
	// or reset by either side.
	ErrStreamClosed = errors.New("rdt: stream closed")
	// ErrTooManyStreams is returned when the peer or local side exceeds the
	// configured live-stream limit.
	ErrTooManyStreams = errors.New("rdt: too many streams")
)

type connState uint8

const (
	connOpen connState = iota
	connClosed
)

// Conn is a reliable connection that carries several independent,
// bidirectional byte streams over one unreliable Transport. Each stream
// owns its own sequence space and receive window; a stalled or cancelled
// stream never blocks the others, and buffers never grow past Config.
type Conn struct {
	conn     Transport
	cfg      Config
	epoch    uint64
	isClient bool

	mu      sync.Mutex
	cond    *sync.Cond
	state   connState
	closed  bool
	recvErr error

	streams    map[uint32]*Stream
	tombstones map[uint32]struct{} // fully finished ids: any late frame drops
	nextID     uint32
	hshakeWin  uint64 // peer window learned from SYN/SYNACK
	rrHint     uint32 // round-robin fairness cursor across senders

	accept []*Stream // inbound streams not yet taken by AcceptStream

	timerDead time.Time
	probeDead map[uint32]time.Time
	rxFrames  chan Frame
	txFrames  chan Frame
	txCtrl    chan Frame
	done      chan struct{}
	rxDone    chan struct{}
	wg        sync.WaitGroup
}

// DialConn opens a multiplexed connection as the active side.
func DialConn(parent context.Context, t Transport, cfg Config) (*Conn, error) {
	c, err := newConn(t, cfg, true, connRandomEpoch())
	if err != nil {
		return nil, err
	}
	if err := c.handshakeClient(parent); err != nil {
		return nil, err
	}
	c.start()
	return c, nil
}

// AcceptConn completes a multiplexed connection as the passive side.
func AcceptConn(parent context.Context, t Transport, cfg Config) (*Conn, error) {
	c, err := newConn(t, cfg, false, 0)
	if err != nil {
		return nil, err
	}
	if err := c.handshakeServer(parent); err != nil {
		return nil, err
	}
	c.start()
	return c, nil
}

func newConn(t Transport, cfg Config, isClient bool, epoch uint64) (*Conn, error) {
	if t == nil {
		return nil, errors.New("rdt: nil transport")
	}
	c := &Conn{
		conn:       t,
		cfg:        cfg.withDefaults(),
		epoch:      epoch,
		isClient:   isClient,
		streams:    make(map[uint32]*Stream),
		tombstones: make(map[uint32]struct{}),
		nextID:     1,
		probeDead:  make(map[uint32]time.Time),
		rxFrames:   make(chan Frame, 256),
		txFrames:   make(chan Frame, 4096),
		txCtrl:     make(chan Frame, 4096),
		done:       make(chan struct{}),
		rxDone:     make(chan struct{}),
	}
	if !isClient {
		c.nextID = 2
	}
	c.cond = sync.NewCond(&c.mu)
	c.wg.Add(1)
	go c.readLoop()
	c.wg.Add(1)
	go c.writeLoop()
	return c, nil
}

func connRandomEpoch() uint64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	v := binary.LittleEndian.Uint64(b[:])
	if v == 0 {
		v = 1
	}
	return v
}

func (c *Conn) send(f Frame) {
	f.Epoch = c.epoch
	ch := c.txFrames
	if f.Kind != KindData {
		ch = c.txCtrl
	}
	select {
	case ch <- f:
	case <-c.done:
	}
}

// sendLocked enqueues a frame under the connection mutex. On local
// saturation the frame is dropped; retransmission makes that harmless.
func (c *Conn) sendLocked(f Frame) {
	f.Epoch = c.epoch
	ch := c.txFrames
	if f.Kind != KindData {
		ch = c.txCtrl
	}
	select {
	case ch <- f:
	default:
	}
}

func (c *Conn) writeLoop() {
	defer c.wg.Done()
	for {
		select {
		case f := <-c.txCtrl:
			_ = c.conn.Send(f)
			continue
		default:
		}
		select {
		case f := <-c.txCtrl:
			_ = c.conn.Send(f)
		case f := <-c.txFrames:
			_ = c.conn.Send(f)
		case <-c.done:
			return
		}
	}
}

func (c *Conn) readLoop() {
	defer close(c.rxDone)
	for {
		f, err := c.conn.Recv()
		if err != nil {
			return
		}
		select {
		case c.rxFrames <- f:
		case <-c.done:
			return
		}
	}
}

// OpenStream creates a locally initiated stream. Client ids are odd,
// server ids are even, so both sides can open streams without collision.
func (c *Conn) OpenStream() (*Stream, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.state != connOpen {
		return nil, ErrClosed
	}
	if len(c.streams)-len(c.tombstones) >= c.cfg.MaxStreams {
		return nil, ErrTooManyStreams
	}
	id := c.nextID
	c.nextID += 2
	st := newStream(c, id)
	c.streams[id] = st
	return st, nil
}

// AcceptStream returns the next stream the peer opened. It blocks until a
// stream arrives or the connection ends.
func (c *Conn) AcceptStream() (*Stream, error) {
	c.mu.Lock()
	for len(c.accept) == 0 && !c.closed {
		c.cond.Wait()
	}
	if len(c.accept) > 0 {
		st := c.accept[0]
		c.accept = c.accept[1:]
		c.mu.Unlock()
		return st, nil
	}
	err := c.recvErr
	c.mu.Unlock()
	if err == nil {
		err = ErrClosed
	}
	return nil, err
}

// Stream returns a locally known stream, or nil if it never existed or
// already finished and was forgotten.
func (c *Conn) Stream(id uint32) *Stream {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.streams[id]
}

// bootstrapStream registers/returns the fixed stream 1 used by the
// single-stream Session facade.
func (c *Conn) bootstrapStream() *Stream {
	c.mu.Lock()
	defer c.mu.Unlock()
	if st := c.streams[1]; st != nil {
		return st
	}
	if c.isClient {
		if c.nextID == 1 {
			c.nextID = 3
		}
	} else {
		if c.nextID == 2 {
			c.nextID = 4
		}
	}
	st := newStream(c, 1)
	c.streams[1] = st
	return st
}

// onHandshakeWindow seeds the advertised receive window for streams that
// already exist when the handshake frame arrives (notably bootstrap
// stream 1), then performs an initial fair-scheduling pass.
func (c *Conn) onHandshakeWindow(win uint64) {
	if win == 0 {
		win = uint64(c.cfg.RecvBuffer)
	}
	c.mu.Lock()
	c.hshakeWin = win
	for _, st := range c.streams {
		st.mu.Lock()
		st.sndWin = win
		st.pumpLocked(-1)
		st.mu.Unlock()
	}
	c.mu.Unlock()
}
