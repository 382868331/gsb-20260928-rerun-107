package rdt

import (
	"io"
	"time"
)

func (s *Session) start() {
	s.wg.Add(1)
	go s.eventLoop()
}

// kick wakes a blocked Write after window/buffer space may have changed.
func (s *Session) kick() { s.cond.Broadcast() }

func (s *Session) eventLoop() {
	defer s.wg.Done()
	// A fixed-tick timer wakes the loop to check RTO/probe deadlines.
	// It is never stopped/reset, avoiding any timer-channel bookkeeping
	// while holding the session mutex.
	tick := time.NewTicker(s.cfg.RTO / 4)
	defer tick.Stop()
	for {
		s.mu.Lock()
		st := s.state
		s.mu.Unlock()
		if st == stateClosed {
			return
		}

		select {
		case <-s.done:
			return
		case <-s.rxDone:
			s.failTransport(io.ErrUnexpectedEOF)
			return
		case f := <-s.rxFrames:
			s.handleFrame(f)
		case <-tick.C:
			s.maybeRTO()
		}
	}
}

func (s *Session) failTransport(err error) {
	s.mu.Lock()
	s.state = stateClosed
	s.closed = true
	s.recvErr = err
	s.kick()
	s.mu.Unlock()
}

func (s *Session) handleFrame(f Frame) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	// Epoch isolation: every frame from an old incarnation is dropped.
	if f.Epoch != s.epoch {
		return
	}
	switch f.Kind {
	case KindData:
		s.onData(f)
	case KindAck, KindSynAck:
		s.onAck(f)
	case KindFin:
		s.onFin(f)
	case KindSyn:
		// Retransmitted SYN (our SYNACK was lost): answer again.
		s.sendLocked(Frame{Kind: KindSynAck, Window: s.availWindowLocked()})
	case KindFinAck:
		s.onFinAck(f)
	}
}

// ---- receive side -------------------------------------------------------

// rcvAvail is total space for new bytes: ordered buffer + reorder hold.
func (s *Session) rcvUsed() uint64 {
	return uint64(len(s.rcvBuf)) + s.rcvHoldBytes
}

func (s *Session) sendAckLocked() {
	s.sendLocked(Frame{
		Kind:   KindAck,
		Ack:    s.rcvNxt,
		Window: uint64(s.cfg.RecvBuffer) - s.rcvUsed(),
	})
}

func (s *Session) onData(f Frame) {
	n := f.length()
	if n == 0 {
		s.sendAckLocked() // pure duplicate probe
		return
	}
	// A segment starting before rcvNxt can still carry NEW bytes when it
	// straddles the delivery frontier (a retransmitted segment). Split off
	// the already-delivered prefix; only a wholly-old segment is dropped.
	if f.Seq < s.rcvNxt {
		if f.Seq+n <= s.rcvNxt {
			s.sendAckLocked()
			return
		}
		f.Payload = f.Payload[s.rcvNxt-f.Seq:]
		f.Seq = s.rcvNxt
		n = f.length()
	}
	if f.Seq > s.rcvNxt {
		// Out of order. Store only what fits the bounded receive buffer;
		// insertHeld dedupes and merges overlapping retransmissions.
		free := uint64(s.cfg.RecvBuffer) - s.rcvUsed()
		if n > free {
			n = free
		}
		if n == 0 {
			s.sendAckLocked()
			return
		}
		var added uint64
		s.rcvHold, added = insertHeld(s.rcvHold, f.Seq, f.Payload[:n])
		s.rcvHoldBytes += added
		s.sendAckLocked()
		return
	}
	// In order: append, then merge any now-contiguous held segments.
	off := s.rcvNxt - f.Seq
	data := f.Payload[off:]
	free := uint64(s.cfg.RecvBuffer) - s.rcvUsed()
	if uint64(len(data)) > free {
		data = data[:free]
	}
	s.rcvBuf = append(s.rcvBuf, data...)
	s.rcvNxt += uint64(len(data))
	s.mergeHold()
	s.sendAckLocked()
	s.kick() // wake Read
}

// mergeHold appends held segments as they become contiguous, trimming
// overlaps with already-accepted bytes. Hold space stays bounded.
func (s *Session) mergeHold() {
	for {
		// Drop fully-consumed segments left at the front.
		for len(s.rcvHold) > 0 && s.rcvHold[0].end() <= s.rcvNxt {
			h0 := s.rcvHold[0]
			s.rcvHoldBytes -= uint64(len(h0.data))
			s.rcvHold = s.rcvHold[1:]
		}
		if len(s.rcvHold) == 0 || s.rcvHold[0].seq > s.rcvNxt ||
			s.rcvHold[0].end() <= s.rcvNxt {
			return
		}
		h := s.rcvHold[0]
		s.rcvHold = s.rcvHold[1:]
		s.rcvHoldBytes -= uint64(len(h.data))
		seg := h.data
		off := s.rcvNxt - h.seq
		seg = seg[off:]
		free := uint64(s.cfg.RecvBuffer) - s.rcvUsed()
		if uint64(len(seg)) > free {
			seg = seg[:free]
		}
		s.rcvBuf = append(s.rcvBuf, seg...)
		s.rcvNxt += uint64(len(seg))
	}
}
