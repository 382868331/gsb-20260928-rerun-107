package rdt

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"sync"
	"time"
)

var (
	// ErrClosed is returned after Close or context cancellation.
	ErrClosed = errors.New("rdt: session closed")
	// ErrHandshake means the handshake did not complete in time.
	ErrHandshake = errors.New("rdt: handshake failed")
)

type state uint8

const (
	stateOpen    state = iota
	stateFinWait       // active closer: FIN sent, awaiting final Ack
	stateClosed
)

// Session is a reliable, ordered byte stream over an unreliable Transport.
// Future congestion strategies can replace the send pacing while keeping
// the same session, cumulative-ack and epoch semantics.
type Session struct {
	conn  Transport
	cfg   Config
	epoch uint64

	mu      sync.Mutex
	cond    *sync.Cond
	state   state
	closed  bool
	readEOF bool
	peerFin bool
	recvErr error

	// send side
	sndUna        uint64             // oldest unacknowledged byte
	sndNxt        uint64             // next byte to hand to the transport
	sndWin        uint64             // peer advertised receive window
	segments      map[uint64]*outSeg // in-flight segments keyed by Seq
	queued        []byte             // accepted but not yet sent
	finQueued     bool
	finSent       bool
	finQuietUntil time.Time

	// receive side
	rcvNxt       uint64    // next in-order byte expected
	rcvBuf       []byte    // in-order bytes not yet Read
	rcvHold      []heldSeg // out-of-order, disjoint, sorted
	rcvHoldBytes uint64

	timerDead  time.Time
	probeDead  time.Time
	finAckDead time.Time

	rxFrames chan Frame
	txFrames chan Frame
	txCtrl   chan Frame
	done     chan struct{}
	rxDone   chan struct{}
	wg       sync.WaitGroup
}

type outSeg struct {
	seq    uint64
	data   []byte
	sentAt time.Time
}

// Dial opens a session as the active (client) side over conn.
func Dial(parent context.Context, conn Transport, cfg Config) (*Session, error) {
	s, err := newSession(conn, cfg, true, randomEpoch())
	if err != nil {
		return nil, err
	}
	if err := s.handshakeClient(parent); err != nil {
		return nil, err
	}
	s.start()
	return s, nil
}

// Accept completes a session as the passive (server) side over conn.
func Accept(parent context.Context, conn Transport, cfg Config) (*Session, error) {
	s, err := newSession(conn, cfg, false, 0)
	if err != nil {
		return nil, err
	}
	if err := s.handshakeServer(parent); err != nil {
		return nil, err
	}
	s.start()
	return s, nil
}

func randomEpoch() uint64 {
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

func newSession(conn Transport, cfg Config, _ bool, epoch uint64) (*Session, error) {
	if conn == nil {
		return nil, errors.New("rdt: nil transport")
	}
	s := &Session{
		conn:     conn,
		cfg:      cfg.withDefaults(),
		epoch:    epoch,
		sndWin:   0,
		segments: make(map[uint64]*outSeg),
		rxFrames: make(chan Frame, 256),
		txFrames: make(chan Frame, 4096),
		txCtrl:   make(chan Frame, 4096),
		done:     make(chan struct{}),
		rxDone:   make(chan struct{}),
	}
	s.cond = sync.NewCond(&s.mu)
	s.wg.Add(1)
	go s.readLoop()
	s.wg.Add(1)
	go s.writeLoop()
	return s, nil
}

func (s *Session) send(f Frame) {
	f.Epoch = s.epoch
	if f.Kind != KindData {
		select {
		case s.txCtrl <- f:
		case <-s.done:
		}
		return
	}
	select {
	case s.txFrames <- f:
	case <-s.done:
	}
}

// sendLocked enqueues a frame while the state mutex is held. On local
// saturation the frame is dropped; retransmission makes that harmless.
func (s *Session) sendLocked(f Frame) {
	f.Epoch = s.epoch
	if f.Kind != KindData {
		select {
		case s.txCtrl <- f:
		default:
		}
		return
	}
	select {
	case s.txFrames <- f:
	default:
	}
}

// writeLoop owns every transport write so a blocked/slow transport can
// never hold up the event loop or a session mutex.
func (s *Session) writeLoop() {
	defer s.wg.Done()
	for {
		// Control frames (acks/syn/fin) always jump the data queue.
		select {
		case f := <-s.txCtrl:
			_ = s.conn.Send(f)
			continue
		default:
		}
		select {
		case f := <-s.txCtrl:
			_ = s.conn.Send(f)
		case f := <-s.txFrames:
			_ = s.conn.Send(f)
		case <-s.done:
			return
		}
	}
}

// readLoop feeds transport frames into the event loop.
func (s *Session) readLoop() {
	defer close(s.rxDone)
	for {
		f, err := s.conn.Recv()
		if err != nil {
			return
		}
		select {
		case s.rxFrames <- f:
		case <-s.done:
			return
		}
	}
}
