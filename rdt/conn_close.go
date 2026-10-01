package rdt

import (
	"context"
)

// forgetStreamLocked drops every buffer and timer for a finished stream
// and records a bounded tombstone so late repeats cannot recreate it.
func (c *Conn) forgetStreamLocked(s *Stream) {
	if _, ok := c.tombstones[s.ID]; ok {
		return
	}
	c.tombstones[s.ID] = struct{}{}
	delete(c.streams, s.ID)
	delete(c.probeDead, s.ID)
	for i, st := range c.accept {
		if st == s {
			c.accept = append(c.accept[:i], c.accept[i+1:]...)
			break
		}
	}
	// Keep tombstones bounded: the oldest ids are the first to be irrelevant.
	for len(c.tombstones) > c.cfg.MaxStreams*2 {
		var oldest uint32 = ^uint32(0)
		for id := range c.tombstones {
			if id < oldest {
				oldest = id
			}
		}
		if oldest == ^uint32(0) {
			break
		}
		delete(c.tombstones, oldest)
	}
	c.cond.Broadcast()
}

// maybeFinishLocked frees a stream once both directions have ended
// locally (FinAck received) and the peer FIN plus all its data arrived.
func (s *Stream) maybeFinishLocked() {
	if s.state == stClosing && s.peerFin {
		s.state = stClosed
		s.readEOF = true
		s.cond.Broadcast()
		s.c.forgetStreamLocked(s)
	}
}

// Close aborts the whole connection: every blocked Read/Write/Accept on
// every stream unwinds and the transport is closed. Per-stream cancellation
// should use Stream.Close when only one direction/stream is done.
func (c *Conn) Close(ctx context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.state = connClosed
	c.closed = true
	c.recvErr = ErrClosed
	for _, st := range c.streams {
		st.resetLocked()
	}
	c.cond.Broadcast()
	c.mu.Unlock()
	select {
	case <-c.done:
	default:
		close(c.done)
	}
	_ = c.conn.Close()
	_ = ctx
	return nil
}
