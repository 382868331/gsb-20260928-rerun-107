package rdt

import (
	"context"
	"io"
	"time"
)

type streamState struct {
	id    uint32
	group uint32

	sndUna        uint64
	sndNxt        uint64
	sndWin        uint64
	segments      map[uint64]*outSeg
	queued        []qChunk
	finQueued     bool
	finSent       bool
	localAcked    bool
	localFinished bool
	writeClosed   bool
	resetSent     bool
	finQuietUntil time.Time

	rcvNxt       uint64
	rcvBuf       []byte
	rcvHold      []heldSeg
	rcvHoldBytes uint64
	readEOF      bool
	peerFin      bool
	peerReset    bool

	timerDead  time.Time
	probeDead  time.Time
	finAckDead time.Time
	finAcked   chan struct{}
	peerDone   chan struct{}

	statSent uint64
	statPend uint64
	statDrop uint64
}

func newStreamState(id uint32, initialWindow uint64) *streamState {
	return &streamState{
		id:       id,
		sndWin:   initialWindow,
		segments: make(map[uint64]*outSeg),
		finAcked: make(chan struct{}),
		peerDone: make(chan struct{}),
	}
}

func (st *streamState) outstanding() uint64 {
	n := uint64(0)
	for _, ch := range st.queued {
		n += uint64(len(ch.data))
	}
	for _, seg := range st.segments {
		n += uint64(len(seg.data))
	}
	return n
}

func (st *streamState) closedForIO() bool {
	return st.peerReset || st.resetSent || st.writeClosed
}

// Stream is one independent bidirectional byte stream multiplexed on a Session.
type Stream struct {
	s  *Session
	id uint32
}

// ID returns the stream's connection-unique identifier.
func (st *Stream) ID() uint32 { return st.id }

// Read reads bytes from this stream. A return of io.EOF means only the
// peer's send direction ended; the local send direction remains usable.
func (st *Stream) Read(p []byte) (int, error) {
	return st.s.readStream(st.id, p)
}

// Write queues data for this stream. A slow reader on another stream does
// not consume this stream's receive window or delivery buffer.
func (st *Stream) Write(p []byte) (int, error) {
	return st.s.writeStream(st.id, p)
}

// CloseWrite performs a directional half-close. It reliably flushes the
// queued data and FIN, but leaves Read available until the peer finishes.
func (st *Stream) CloseWrite(ctx context.Context) error {
	return st.s.closeWriteStream(ctx, st.id, false)
}

// Close abandons this stream only. Other streams remain usable.
func (st *Stream) Close() error {
	return st.s.resetStream(st.id, ErrClosed)
}

// SetGroup moves this stream to a business group for FUTURE writes only.
// Bytes already queued or in flight stay attributed to the group they
// were sent under, so switching groups cannot free or duplicate quota.
func (st *Stream) SetGroup(group uint32) { st.s.setStreamGroup(st.id, group) }

// Stats returns this stream's send accounting snapshot.
func (st *Stream) Stats() StreamStats {
	s, _ := st.s.StreamStats(st.id)
	return s
}

var _ io.Reader = (*Stream)(nil)
var _ io.Writer = (*Stream)(nil)
