package rdt

import (
	"context"
	"time"
)

// writableLocked reports whether application bytes may still be accepted.
func (s *Stream) writableLocked() bool {
	if s.reset || s.finQueued {
		return false
	}
	switch s.state {
	case stOpen:
		return true
	default:
		return false
	}
}

// pumpLocked moves queued bytes into Data segments while the per-stream
// peer window allows, sending at most quota segments in one scheduler
// turn. Fairness across streams comes from the Conn scheduler rotating
// this turn round-robin, not from any per-stream priority.
func (s *Stream) pumpLocked(quota int) int {
	sent := 0
	for quota < 0 || sent < quota {
		used := s.sndNxt - s.sndUna
		effWin := s.sndWin
		if effWin > uint64(s.c.cfg.SendBuffer) {
			effWin = uint64(s.c.cfg.SendBuffer)
		}
		if used >= effWin || len(s.queued) == 0 {
			break
		}
		avail := effWin - used
		n := s.c.cfg.MaxSegment
		if n > len(s.queued) {
			n = len(s.queued)
		}
		if uint64(n) > avail {
			n = int(avail)
		}
		data := make([]byte, n)
		copy(data, s.queued[:n])
		s.queued = s.queued[n:]
		seg := &outSeg{seq: s.sndNxt, data: data, sentAt: time.Now()}
		s.segs[seg.seq] = seg
		s.sndNxt += uint64(n)
		s.c.sendLocked(Frame{Kind: KindData, Stream: s.ID, Seq: seg.seq, Payload: data})
		sent++
	}

	// FIN is sequenced strictly after every byte went to the wire and was
	// acknowledged, so half-close never truncates in-flight data.
	if s.finQueued && !s.finSent && len(s.queued) == 0 && len(s.segs) == 0 {
		s.finSent = true
		s.finSeq = s.sndNxt
		s.c.sendLocked(Frame{Kind: KindFin, Stream: s.ID, Seq: s.finSeq, Ack: s.rcvNxt, Window: s.availWindowLocked()})
	}
	return sent
}

// Write accepts bytes for this stream only. Blocking is bounded by the
// per-stream SendBuffer; other streams keep flowing while this one waits.
func (s *Stream) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	written := 0
	for len(p) > 0 {
		s.mu.Lock()
		for !s.reset && s.writableLocked() &&
			s.outstandingLocked() >= uint64(s.c.cfg.SendBuffer) {
			s.cond.Wait()
		}
		if s.reset || !s.writableLocked() {
			s.mu.Unlock()
			return written, ErrStreamClosed
		}
		space := uint64(s.c.cfg.SendBuffer) - s.outstandingLocked()
		n := len(p)
		if uint64(n) > space {
			n = int(space)
		}
		s.queued = append(s.queued, p[:n]...)
		s.pumpLocked(-1)
		s.mu.Unlock()
		p = p[n:]
		written += n
	}
	return written, nil
}

// CloseWrite ends only the send direction: queued data and FIN are
// reliably delivered, while Read keeps accepting the peer's data until
// its FIN or RST. It does not wait for the peer.
func (s *Stream) CloseWrite() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reset {
		return ErrStreamClosed
	}
	if s.finQueued {
		return nil
	}
	s.finQueued = true
	if s.state == stOpen {
		s.state = stFinWait
	}
	s.pumpLocked(-1)
	s.cond.Broadcast()
	return nil
}

// Close cancels the stream locally: blocked Read/Write unwind, buffers are
// released immediately (no unbounded buffering of a dead stream) and RST
// informs the peer. Other streams on the Conn are untouched. Idempotent.
func (s *Stream) Close() error {
	s.c.mu.Lock()
	defer s.c.mu.Unlock()
	s.mu.Lock()
	already := s.reset
	s.mu.Unlock()
	if already {
		return nil
	}
	s.resetLocked()
	s.c.sendLocked(Frame{Kind: KindRst, Stream: s.ID})
	s.c.forgetStreamLocked(s)
	return nil
}

func (s *Stream) resetLocked() {
	s.mu.Lock()
	s.reset = true
	s.state = stClosed
	s.queued = nil
	s.segs = map[uint64]*outSeg{}
	s.rcvBuf = nil
	s.rcvHold = nil
	s.rcvHoldBytes = 0
	s.cond.Broadcast()
	s.mu.Unlock()
}

// CloseAndWait gracefully closes the stream: it half-closes the send
// direction (queued data and FIN are reliably delivered and acknowledged)
// and then waits until the peer FIN and every byte before it arrived.
// Read still drains inbound data while this runs. Cancellation resets
// only this stream with RST; sibling streams and the connection continue.
func (s *Stream) CloseAndWait(ctx context.Context) error {
	if err := s.CloseWrite(); err != nil {
		return err
	}
	ticker := time.NewTicker(s.c.cfg.RTO / 2)
	defer ticker.Stop()
	for {
		s.mu.Lock()
		done := s.state == stClosed || s.reset
		s.mu.Unlock()
		if done {
			return nil
		}
		select {
		case <-ctx.Done():
			_ = s.Close()
			return ctx.Err()
		case <-s.c.done:
			return ErrClosed
		case <-ticker.C:
			s.c.mu.Lock()
			s.c.onRTOLocked(time.Now())
			s.c.mu.Unlock()
		}
	}
}
