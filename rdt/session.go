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
	recvErr error

	streams       map[uint32]*streamState
	streamOrder   []uint32
	sendCursor    int
	nextLocalID   uint32
	maxStreams    int
	remoteStreams int
	acceptedCh    chan *Stream

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

const defaultStreamID = uint32(0)

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
		conn:       conn,
		cfg:        cfg.withDefaults(),
		epoch:      epoch,
		streams:    make(map[uint32]*streamState),
		acceptedCh: make(chan *Stream, 128),
		rxFrames:   make(chan Frame, 256),
		txFrames:   make(chan Frame, 4096),
		txCtrl:     make(chan Frame, 4096),
		done:       make(chan struct{}),
		rxDone:     make(chan struct{}),
	}
	s.maxStreams = s.cfg.MaxStreams
	if s.maxStreams <= 0 {
		s.maxStreams = 1024
	}
	s.streams[defaultStreamID] = newStreamState(defaultStreamID, 0)
	s.streamOrder = append(s.streamOrder, defaultStreamID)
	s.nextLocalID = 1
	s.cond = sync.NewCond(&s.mu)
	s.wg.Add(1)
	go s.readLoop()
	s.wg.Add(1)
	go s.writeLoop()
	return s, nil
}

// OpenStream creates a locally initiated independent bidirectional stream.
func (s *Session) OpenStream() *Stream {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.nextLocalID
	s.nextLocalID += 2
	s.streams[id] = newStreamState(id, uint64(s.cfg.RecvBuffer))
	s.streamOrder = append(s.streamOrder, id)
	return &Stream{s: s, id: id}
}

// AcceptStream returns a stream initiated by the peer. The default stream
// is implicit and is not delivered here.
func (s *Session) AcceptStream() (*Stream, error) {
	select {
	case st := <-s.acceptedCh:
		return st, nil
	case <-s.done:
		return nil, ErrClosed
	}
}

func (s *Session) getOrCreatePeerStreamLocked(id uint32) *streamState {
	if st, ok := s.streams[id]; ok {
		return st
	}
	if id == defaultStreamID || s.remoteStreams >= s.maxStreams {
		return nil
	}
	st := newStreamState(id, uint64(s.cfg.RecvBuffer))
	s.streams[id] = st
	s.streamOrder = append(s.streamOrder, id)
	s.remoteStreams++
	select {
	case s.acceptedCh <- &Stream{s: s, id: id}:
	default:
	}
	return st
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
