package rdt

import (
	"context"
	"time"
)

// Close half-closes the legacy stream send direction. Receiving stays open.
func (s *Session) Close(ctx context.Context) error {
	return s.closeWriteStream(ctx, defaultStreamID, false)
}

// CloseAndWait flushes and waits for FINACK or peer close.
func (s *Session) CloseAndWait(ctx context.Context) error {
	s.mu.Lock()
	st := s.streams[defaultStreamID]
	passive := st != nil && st.peerFin && !st.finSent
	s.mu.Unlock()
	if passive {
		return s.closePassive(ctx)
	}
	return s.closeWriteStream(ctx, defaultStreamID, true)
}

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
			st := s.streams[defaultStreamID]
			quiet := st == nil || !time.Now().Before(st.finQuietUntil)
			if quiet && st != nil && len(st.rcvBuf) == 0 {
				st.readEOF = true
			}
			if quiet && st != nil {
				st.writeClosed = true
				st.localFinished = true
				s.maybeFinishLocked()
			}
			s.mu.Unlock()
			if quiet {
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
	for _, st := range s.streams {
		st.peerReset = true
	}
	close(s.done)
	s.kick()
	s.mu.Unlock()
	_ = s.conn.Close()
}

// Abort terminates the entire connection.
func (s *Session) Abort() { s.abort() }
