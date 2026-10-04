package rdt

import (
	"context"
	"io"
	"time"
)

type streamState struct {
	id uint32

	group                            uint32
	statSent, statAcked, statDropped uint64

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

// queuedChunk is a run of written bytes pinned to the business group the
// stream belonged to when they were queued. Later group switches only
// append new chunks; earlier bytes keep their original attribution
// through segmentation, retransmission, acknowledgement and cancel.
type queuedChunk struct {
	group uint32
	data  []byte
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
	var n uint64
	for _, c := range st.queued {
		n += uint64(len(c.data))
	}
	for _, seg := range st.segments {
		n += uint64(len(seg.data))
	}
	return n
}

func (st *streamState) closedForIO() bool {
	return st.peerReset || st.resetSent || st.writeClosed
}

// sendGroupLocked is the group the next transmitted segment is charged
// to: the group pinned to the oldest queued bytes, or the stream's
// current group when nothing is queued.
func (st *streamState) sendGroupLocked() uint32 {
	if len(st.queued) > 0 {
		return st.queued[0].group
	}
	return st.group
}

// Stream is one independent bidirectional byte stream multiplexed on a Session.
type Stream struct {
	s  *Session
	id uint32
}

// ID returns the stream's connection-unique identifier.
func (st *Stream) ID() uint32 { return st.id }

// SetGroup moves the stream to a business group. Only writes made after
// the switch are charged to the new group; earlier occupancy,
// acknowledgements and cancellation stay with the group that queued them.
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
