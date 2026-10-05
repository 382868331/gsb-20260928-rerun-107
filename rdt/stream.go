package rdt

import (
	"context"
	"io"
	"time"
)

type streamState struct {
	id uint32

	group                            uint32
	statSent, statAcked              uint64
	statDroppedInflight              uint64
	statDroppedQueued                uint64

	sndUna        uint64
	sndNxt        uint64
	sndWin        uint64
	segments      map[uint64]*outSeg
	queued        []queuedChunk
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

// queuedChunk is a run of written bytes pinned to the business group the
// stream belonged to when they were queued. Later group switches only
// append new chunks, so a segment formed from a chunk keeps the group of
// its write.
type queuedChunk struct {
	group uint32
	data  []byte
}

func (st *streamState) queuedLen() int {
	n := 0
	for _, c := range st.queued {
		n += len(c.data)
	}
	return n
}

func (st *streamState) outstanding() uint64 {
	n := uint64(st.queuedLen())
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

// SetGroup moves the stream to a business group. Only bytes written after
// the switch are charged to the new group; earlier queued and sent bytes
// keep their original group, and occupancy, acknowledgements and
// cancellation stay with the group that sent them.
func (st *Stream) SetGroup(g uint32) { _ = st.s.SetStreamGroup(st.id, g) }

// Group returns the stream's current business group.
func (st *Stream) Group() uint32 {
	st.s.mu.Lock()
	defer st.s.mu.Unlock()
	if st2 := st.s.streams[st.id]; st2 != nil {
		return st2.group
	}
	return 0
}

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

var _ io.Reader = (*Stream)(nil)
var _ io.Writer = (*Stream)(nil)
