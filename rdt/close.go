package rdt

import (
	"context"
	"time"
)

// Close flushes queued data, sends FIN and waits for the peer's FinAck.
// It is idempotent. If the peer already sent its FIN first, Close lingers
// and answers retransmitted FINs until the peer goes quiet (meaning it
// received a FinAck) or ctx is cancelled, which then aborts the session
// and unwinds blocked Read/Write on both sides.
func (s *Session) Close(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	if s.peerFin {
		s.mu.Unlock()
		return s.closePassive(ctx)
	}
	s.finQueued = true
	s.state = stateFinWait
	s.kick()
	s.mu.Unlock()

	ticker := time.NewTicker(s.cfg.RTO)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			s.abort()
			return ctx.Err()
		case <-s.done:
			return nil
		case <-ticker.C:
			// Retransmit any still-unacked data and the FIN once sequenced.
			s.mu.Lock()
			now := time.Now()
			for _, seg := range s.segments {
				if now.Sub(seg.sentAt) >= s.cfg.RTO/2 {
					seg.sentAt = now
					s.sendLocked(Frame{Kind: KindData, Seq: seg.seq, Payload: seg.data})
				}
			}
			if s.finSent {
				s.sendLocked(Frame{Kind: KindFin, Seq: s.sndNxt})
			}
			s.mu.Unlock()
		}
	}
}

// closePassive keeps answering FIN retransmits until the peer is quiet
// for CloseGrace, so the active side reliably gets at least one FinAck.
func (s *Session) closePassive(ctx context.Context) error {
	tick := time.NewTicker(s.cfg.RTO / 2)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			s.abort()
			return ctx.Err()
		case <-tick.C:
			s.mu.Lock()
			quiet := !time.Now().Before(s.finQuietUntil)
			s.mu.Unlock()
			if quiet {
				s.finishPassive()
				return nil
			}
		}
	}
}

func (s *Session) abort() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.state = stateClosed
	s.closed = true
	s.recvErr = ErrClosed
	close(s.done)
	s.kick()
	s.mu.Unlock()
	// Tear down the transport so the peer's Recv unblocks and both sides
	// wind down. The unreliable transport is shared by design; cancellation
	// means the whole connection is over.
	_ = s.conn.Close()
}

// finishPassive ends a session that only answered the peer's FIN.
func (s *Session) finishPassive() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.state = stateClosed
	s.closed = true
	s.readEOF = true
	close(s.done)
	s.kick()
	s.mu.Unlock()
}
