package rdt

import (
	"context"
	"io"
	"time"
)

func (s *Session) pumpAllLocked() {
	if len(s.streamOrder) == 0 {
		return
	}
	for range s.streamOrder {
		if s.sendCursor >= len(s.streamOrder) {
			s.sendCursor = 0
		}
		id := s.streamOrder[s.sendCursor]
		s.sendCursor = (s.sendCursor + 1) % len(s.streamOrder)
		if st := s.streams[id]; st != nil {
			s.pumpStreamLocked(id, st)
		}
	}
}

func (s *Session) pumpStreamLocked(id uint32, st *streamState) bool {
	if st.resetSent || st.peerReset || st.writeClosed {
		return false
	}
	sent := false
	used := st.sndNxt - st.sndUna
	effWin := st.sndWin
	if effWin > uint64(s.cfg.SendBuffer) {
		effWin = uint64(s.cfg.SendBuffer)
	}
	if used < effWin && len(st.queued) > 0 {
		avail := effWin - used
		n := s.cfg.MaxSegment
		if n > len(st.queued) {
			n = len(st.queued)
		}
		if uint64(n) > avail {
			n = int(avail)
		}
		data := make([]byte, n)
		copy(data, st.queued[:n])
		st.queued = st.queued[n:]
		seg := &outSeg{seq: st.sndNxt, data: data, sentAt: time.Now()}
		st.segments[seg.seq] = seg
		st.sndNxt += uint64(n)
		s.sendLocked(Frame{Kind: KindData, Stream: id, Seq: seg.seq, Payload: data})
		st.timerDead = time.Now().Add(s.cfg.RTO)
		sent = true
	}
	if st.finQueued && !st.finSent && len(st.queued) == 0 && len(st.segments) == 0 &&
		(st.sndNxt-st.sndUna == 0) {
		st.finSent = true
		s.sendLocked(Frame{Kind: KindFin, Stream: id, Seq: st.sndNxt, Ack: st.rcvNxt, Window: s.availWindowLocked(id)})
		st.timerDead = time.Now().Add(s.cfg.RTO)
	}
	return sent
}

func (s *Session) maybeRTO() {
	s.mu.Lock()
	now := time.Now()
	due := false
	for _, st := range s.streams {
		if (!st.timerDead.IsZero() && !now.Before(st.timerDead)) ||
			(!st.probeDead.IsZero() && !now.Before(st.probeDead)) ||
			(st.peerFin && len(st.segments) == 0 && !now.Before(st.finAckDead)) {
			due = true
		}
	}
	s.mu.Unlock()
	if due {
		s.onRTO()
	}
	s.mu.Lock()
	s.maybeFinishLocked()
	s.mu.Unlock()
}

func (s *Session) onRTO() {
	if s.closed {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for id, st := range s.streams {
		if st.peerFin && len(st.segments) == 0 && len(st.queued) == 0 && st.localAcked && !st.finSent && !st.resetSent {
			s.sendLocked(Frame{Kind: KindFinAck, Stream: id, Ack: st.rcvNxt, Window: s.availWindowLocked(id)})
			st.finAckDead = now.Add(s.cfg.RTO)
			continue
		}
		if !st.probeDead.IsZero() && !now.Before(st.probeDead) {
			probeSeq := uint64(0)
			if st.sndUna > 0 {
				probeSeq = st.sndUna - 1
			}
			probe := Frame{Kind: KindData, Stream: id, Seq: probeSeq}
			if st.sndUna > 0 {
				probe.Payload = []byte{0}
			}
			s.sendLocked(probe)
			st.probeDead = now.Add(s.cfg.RTO)
		}
		for _, seg := range st.segments {
			if now.Sub(seg.sentAt) >= s.cfg.RTO {
				seg.sentAt = now
				s.sendLocked(Frame{Kind: KindData, Stream: id, Seq: seg.seq, Payload: seg.data})
			}
		}
		if st.finSent {
			s.sendLocked(Frame{Kind: KindFin, Stream: id, Seq: st.sndNxt, Ack: st.rcvNxt, Window: s.availWindowLocked(id)})
		}
		st.timerDead = now.Add(s.cfg.RTO)
	}
	s.pumpAllLocked()
}

func (s *Session) advanceAckLocked(f Frame) {
	st := s.streams[f.Stream]
	if st == nil || st.resetSent || st.peerReset {
		return
	}
	prevWin := st.sndWin
	st.sndWin = f.Window
	if f.Window > 0 {
		st.probeDead = time.Time{}
	} else if (len(st.queued) > 0 || st.finQueued) && st.probeDead.IsZero() {
		st.probeDead = time.Now().Add(s.cfg.RTO)
	}
	if f.Ack > st.sndUna && f.Ack <= st.sndNxt {
		st.sndUna = f.Ack
		for seq, seg := range st.segments {
			if seq+uint64(len(seg.data)) <= st.sndUna {
				delete(st.segments, seq)
			}
		}
		s.kick()
	}
	s.pumpAllLocked()
	if f.Window > prevWin {
		s.kick()
	}
}

func (s *Session) onAck(f Frame) {
	s.advanceAckLocked(f)
}

func (s *Session) onFin(f Frame) {
	st := s.getOrCreatePeerStreamLocked(f.Stream)
	if st == nil || st.peerReset || st.readEOF {
		return
	}
	s.advanceAckLocked(f)
	if f.Seq != st.rcvNxt {
		s.sendAckLocked(f.Stream, st)
		return
	}
	st.rcvNxt = f.Seq + 1
	st.readEOF = true
	st.peerFin = true
	close(st.peerDone)
	s.sendLocked(Frame{Kind: KindFinAck, Stream: f.Stream, Ack: f.Seq + 1, Window: s.availWindowLocked(f.Stream)})
	st.finAckDead = time.Now().Add(s.cfg.RTO)
	st.finQuietUntil = time.Now().Add(s.cfg.CloseGrace)
	s.maybeFinishLocked()
	s.kick()
}

func (s *Session) onFinAck(f Frame) {
	st := s.streams[f.Stream]
	if st == nil {
		return
	}
	if st.finSent && f.Ack == st.sndNxt+1 {
		st.localAcked = true
		select {
		case <-st.finAcked:
		default:
			close(st.finAcked)
		}
		if !st.peerFin {
			st.finQuietUntil = time.Now().Add(s.cfg.CloseGrace)
		}
		s.maybeFinishLocked()
		s.kick()
	}
}

func (s *Session) onRst(f Frame) {
	st := s.streams[f.Stream]
	if st == nil {
		return
	}
	st.peerReset = true
	st.queued = nil
	st.segments = make(map[uint64]*outSeg)
	s.maybeFinishLocked()
	s.kick()
}

func (s *Session) maybeFinishLocked() {
	if s.closed {
		return
	}
	for _, st := range s.streams {
		localDone := st.localFinished || st.resetSent
		peerDone := st.peerFin || st.peerReset
		if !localDone || !peerDone {
			return
		}
	}
	s.state = stateClosed
	s.closed = true
	close(s.done)
}

func (s *Session) availWindowLocked(id uint32) uint64 {
	st := s.streams[id]
	if st == nil {
		return uint64(s.cfg.RecvBuffer)
	}
	return uint64(s.cfg.RecvBuffer) - s.rcvUsed(st)
}

func (s *Session) writeStream(id uint32, p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	written := 0
	for len(p) > 0 {
		s.mu.Lock()
		st := s.streams[id]
		for st != nil && !s.closed && !st.closedForIO() &&
			(st.outstanding() >= uint64(s.cfg.SendBuffer) || (st.sndNxt-st.sndUna >= st.sndWin && st.sndWin > 0)) {
			s.cond.Wait()
			st = s.streams[id]
		}
		if s.closed {
			s.mu.Unlock()
			return written, ErrClosed
		}
		if st == nil || st.closedForIO() || st.finQueued {
			s.mu.Unlock()
			return written, ErrClosed
		}
		space := uint64(s.cfg.SendBuffer) - st.outstanding()
		n := len(p)
		if uint64(n) > space {
			n = int(space)
		}
		st.queued = append(st.queued, p[:n]...)
		s.pumpAllLocked()
		s.mu.Unlock()
		p = p[n:]
		written += n
	}
	return written, nil
}

func (s *Session) readStream(id uint32, p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.streams[id]
	for st != nil && len(st.rcvBuf) == 0 && !st.readEOF && !st.peerReset && !s.closed {
		s.cond.Wait()
		st = s.streams[id]
	}
	if s.closed && s.recvErr != nil {
		return 0, s.recvErr
	}
	if st == nil || st.peerReset {
		return 0, ErrClosed
	}
	if len(st.rcvBuf) > 0 {
		n := copy(p, st.rcvBuf)
		st.rcvBuf = st.rcvBuf[n:]
		s.sendAckLocked(id, st)
		return n, nil
	}
	if st.readEOF {
		return 0, io.EOF
	}
	return 0, ErrClosed
}

func (s *Session) closeWriteStream(ctx context.Context, id uint32, waitAck bool) error {
	s.mu.Lock()
	st := s.streams[id]
	if st == nil {
		s.mu.Unlock()
		return ErrClosed
	}
	if !st.finQueued && !st.writeClosed && !st.resetSent {
		st.finQueued = true
		s.pumpAllLocked()
	}
	s.mu.Unlock()
	finAcked := st.finAcked
	if !waitAck {
		if ctx != nil && ctx.Done() != nil {
			go func() {
				select {
				case <-ctx.Done():
					if id == defaultStreamID {
						s.abort()
					}
				case <-finAcked:
				case <-s.done:
				}
			}()
		}
		return nil
	}
	ticker := time.NewTicker(s.cfg.RTO)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			if id == defaultStreamID {
				s.abort()
			}
			return ctx.Err()
		case <-s.done:
			s.mu.Lock()
			done := st.writeClosed || s.closed
			s.mu.Unlock()
			if done {
				return nil
			}
			return ErrClosed
		case <-finAcked:
			s.mu.Lock()
			if st.peerFin {
				st.localFinished = true
				s.maybeFinishLocked()
			}
			s.mu.Unlock()
			return nil
		case <-ticker.C:
			s.mu.Lock()
			st = s.streams[id]
			done := st == nil || st.writeClosed || st.peerReset
			if st != nil && !done {
				s.pumpAllLocked()
			}
			s.mu.Unlock()
			if done {
				return nil
			}
		}
	}
}

func (s *Session) resetStream(id uint32, err error) error {
	s.mu.Lock()
	st := s.streams[id]
	if st != nil && !st.resetSent && !st.peerReset {
		st.resetSent = true
		st.localAcked = true
		st.queued = nil
		st.segments = make(map[uint64]*outSeg)
		s.sendLocked(Frame{Kind: KindRst, Stream: id})
		s.kick()
	}
	s.mu.Unlock()
	_ = err
	return nil
}

func (s *Session) Write(p []byte) (int, error) {
	return s.writeStream(defaultStreamID, p)
}

func (s *Session) Read(p []byte) (int, error) {
	return s.readStream(defaultStreamID, p)
}

func (s *Session) CloseWrite(ctx context.Context) error {
	_ = ctx
	return s.closeWriteStream(context.Background(), defaultStreamID, false)
}

// WaitForPeerClose waits until the peer has finished its send direction.
func (s *Session) WaitForPeerClose(ctx context.Context) error {
	st := s.streams[defaultStreamID]
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.done:
		return ErrClosed
	case <-st.peerDone:
		return nil
	}
}
