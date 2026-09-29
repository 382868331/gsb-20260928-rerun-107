package rdt

import (
	"io"
	"time"
)

// outstanding counts bytes queued or in flight (send-buffer accounting).
func (s *Session) outstanding() uint64 {
	n := uint64(len(s.queued))
	for _, seg := range s.segments {
		n += uint64(len(seg.data))
	}
	return n
}

// pumpLocked moves queued bytes into Data segments while the peer window
// allows, and sends FIN only once every byte has been transmitted.
func (s *Session) pumpLocked() {
	for {
		used := s.sndNxt - s.sndUna
		effWin := s.sndWin
		if effWin > uint64(s.cfg.SendBuffer) {
			effWin = uint64(s.cfg.SendBuffer)
		}
		if used >= effWin || len(s.queued) == 0 {
			break
		}
		avail := effWin - used
		n := s.cfg.MaxSegment
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
		s.segments[seg.seq] = seg
		s.sndNxt += uint64(n)
		s.sendLocked(Frame{Kind: KindData, Seq: seg.seq, Payload: data})
		s.armTimerLocked()
	}

	// FIN is sequenced strictly after all data has been handed to the wire.
	if s.finQueued && !s.finSent && len(s.queued) == 0 &&
		s.sndNxt-s.sndUna == 0 && len(s.segments) == 0 {
		s.finSent = true
		s.sendLocked(Frame{Kind: KindFin, Seq: s.sndNxt})
		s.armTimerLocked()
	}
}

func (s *Session) armTimerLocked() {
	if len(s.segments) == 0 && !s.finQueued {
		s.timerDead = time.Time{}
		return
	}
	s.timerDead = time.Now().Add(s.cfg.RTO)
}

// maybeRTO fires retransmission/probe/finack work only when its deadline
// has passed; ticks that arrive early are ignored.
func (s *Session) maybeRTO() {
	s.mu.Lock()
	now := time.Now()
	rtoDue := !s.timerDead.IsZero() && !now.Before(s.timerDead)
	probeDue := !s.probeDead.IsZero() && !now.Before(s.probeDead)
	finAckDue := s.peerFin && !now.Before(s.finAckDead)
	s.mu.Unlock()
	if rtoDue || probeDue || finAckDue {
		s.onRTO()
	}
}

func (s *Session) onRTO() {
	if s.closed {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if s.peerFin && len(s.segments) == 0 {
		s.sendLocked(Frame{Kind: KindFinAck, Ack: s.rcvNxt, Window: s.availWindowLocked()})
		s.finAckDead = now.Add(s.cfg.RTO)
		return
	}
	s.probeDead = time.Time{}
	for _, seg := range s.segments {
		if now.Sub(seg.sentAt) >= s.cfg.RTO {
			seg.sentAt = now
			s.sendLocked(Frame{Kind: KindData, Seq: seg.seq, Payload: seg.data})
		}
	}
	if s.finSent {
		s.sendLocked(Frame{Kind: KindFin, Seq: s.sndNxt})
	}
	s.pumpLocked()
	s.timerDead = now.Add(s.cfg.RTO)
}

// sendAckProbeLocked retransmits one already-delivered byte; the receiver
// rejects it as old but answers with a fresh cumulative ack + window.
func (s *Session) sendAckProbeLocked() {
	probeSeq := uint64(0)
	if s.sndUna > 0 {
		probeSeq = s.sndUna - 1
	}
	s.sendLocked(Frame{Kind: KindData, Seq: probeSeq, Payload: []byte{0}})
	s.probeDead = time.Now().Add(s.cfg.RTO)
}

func (s *Session) advanceAckLocked(ack, win uint64) {
	prevWin := s.sndWin
	s.sndWin = win
	if win > 0 {
		s.probeDead = time.Time{}
	} else if (len(s.queued) > 0 || s.finQueued) && s.probeDead.IsZero() {
		s.probeDead = time.Now().Add(s.cfg.RTO)
	}
	if ack > s.sndUna && ack <= s.sndNxt {
		s.sndUna = ack
		for seq, seg := range s.segments {
			if seq+uint64(len(seg.data)) <= s.sndUna {
				delete(s.segments, seq)
			}
		}
		s.kick()
	}
	s.pumpLocked()
	// A window opening (possibly without new cumulative ack) must wake a
	// Write blocked on the send buffer.
	if win > prevWin {
		s.kick()
	}
}

func (s *Session) onAck(f Frame) {
	s.advanceAckLocked(f.Ack, f.Window)
	s.armTimerLocked()
}

func (s *Session) onFin(f Frame) {
	s.advanceAckLocked(f.Ack, f.Window)
	// FIN is the sequence number right after all data; wait for missing data.
	if f.Seq != s.rcvNxt {
		s.sendAckLocked()
		return
	}
	s.rcvNxt = f.Seq + 1
	s.readEOF = true
	s.peerFin = true
	s.sendLocked(Frame{Kind: KindFinAck, Ack: f.Seq + 1, Window: s.availWindowLocked()})
	s.finAckDead = time.Now().Add(s.cfg.RTO)
	// Every retransmitted FIN pushes the quiet deadline out: once the peer
	// gets a FinAck it stops sending FIN, so silence means we can finish.
	s.finQuietUntil = time.Now().Add(s.cfg.CloseGrace)
	s.kick()
}

func (s *Session) onFinAck(f Frame) {
	s.sndWin = f.Window
	s.kick()
	// FIN occupies one sequence number, so its ack is sndNxt+1.
	if s.finSent && f.Ack == s.sndNxt+1 {
		s.markClosedLocked()
	}
}

func (s *Session) availWindowLocked() uint64 {
	return uint64(s.cfg.RecvBuffer) - s.rcvUsed()
}

func (s *Session) markClosedLocked() {
	if s.closed {
		return
	}
	s.state = stateClosed
	s.closed = true
	s.readEOF = true
	close(s.done)
	s.kick()
}

// Write implements io.Writer with blocking flow control and bounded buffers.
func (s *Session) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	written := 0
	for len(p) > 0 {
		s.mu.Lock()
		for !s.closed && s.state == stateOpen &&
			s.outstanding() >= uint64(s.cfg.SendBuffer) {
			s.cond.Wait()
		}
		if s.closed || s.state != stateOpen {
			s.mu.Unlock()
			return written, ErrClosed
		}
		space := uint64(s.cfg.SendBuffer) - s.outstanding()
		n := len(p)
		if uint64(n) > space {
			n = int(space)
		}
		s.queued = append(s.queued, p[:n]...)
		s.pumpLocked()
		s.mu.Unlock()
		p = p[n:]
		written += n
	}
	return written, nil
}

// Read implements io.Reader. Returns io.EOF after the peer closes cleanly.
func (s *Session) Read(p []byte) (int, error) {
	s.mu.Lock()
	for len(s.rcvBuf) == 0 && !s.readEOF && !s.closed {
		s.cond.Wait()
	}
	if len(s.rcvBuf) > 0 {
		n := copy(p, s.rcvBuf)
		s.rcvBuf = s.rcvBuf[n:]
		// Always advertise newly freed space: without a fresh DATA frame to
		// trigger an ack, the sender could stall on a stale small window.
		s.sendAckLocked()
		s.mu.Unlock()
		return n, nil
	}
	if s.closed && s.recvErr != nil {
		err := s.recvErr
		s.mu.Unlock()
		return 0, err
	}
	if s.readEOF || s.closed {
		s.mu.Unlock()
		return 0, io.EOF
	}
	s.mu.Unlock()
	return 0, nil
}
