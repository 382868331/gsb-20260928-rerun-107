package rdt

import (
	"context"
	"time"
)

// Session is the single-stream facade over a multiplexed Conn. Stream 1
// behaves like the historical connection: Read and Write are independent,
// Close half-closes the send direction while still delivering data the
// peer has in flight, and cancellation aborts the connection.
type Session struct {
	c  *Conn
	st *Stream
}

// Dial opens a single-stream session as the active side.
func Dial(parent context.Context, t Transport, cfg Config) (*Session, error) {
	c, err := DialConn(parent, t, cfg)
	if err != nil {
		return nil, err
	}
	return &Session{c: c, st: c.bootstrapStream()}, nil
}

// Accept completes a single-stream session as the passive side.
func Accept(parent context.Context, t Transport, cfg Config) (*Session, error) {
	c, err := AcceptConn(parent, t, cfg)
	if err != nil {
		return nil, err
	}
	return &Session{c: c, st: c.bootstrapStream()}, nil
}

// Conn exposes the underlying multiplexed connection so additional
// independent streams can be opened on the same transport.
func (s *Session) Conn() *Conn { return s.c }

// Stream returns the bootstrap stream (id 1).
func (s *Session) Stream() *Stream { return s.st }

func (s *Session) Write(p []byte) (int, error) { return s.st.Write(p) }

func (s *Session) Read(p []byte) (int, error) { return s.st.Read(p) }

// Close half-closes the send direction (queued bytes and FIN are reliably
// delivered and acknowledged), then keeps the receive direction open long
// enough to drain data the peer is still sending. When both sides have
// finished, or CloseGrace elapses after our FinAck when the peer sends no
// FIN, the connection closes. Cancelling ctx aborts the connection and
// unwinds blocked Read/Write on both sides.
func (s *Session) Close(ctx context.Context) error {
	if err := s.st.CloseWrite(); err != nil {
		return err
	}
	ticker := time.NewTicker(s.c.cfg.RTO / 2)
	defer ticker.Stop()
	var grace *time.Timer
	var graceC <-chan time.Time
	for {
		s.st.mu.Lock()
		finAcked := s.st.state == stClosing || s.st.state == stClosed
		peerDone := s.st.peerFin
		s.st.mu.Unlock()
		if finAcked && grace == nil {
			grace = time.NewTimer(s.c.cfg.CloseGrace)
			graceC = grace.C
		}
		if peerDone {
			// All bytes preceding the peer FIN are already in the receive
			// buffer and visible to Read (which returned/will return EOF).
			if grace != nil {
				grace.Stop()
			}
			return s.c.Close(context.Background())
		}
		select {
		case <-ctx.Done():
			_ = s.c.Close(context.Background())
			return ctx.Err()
		case <-s.c.done:
			return nil
		case <-graceC:
			return s.c.Close(context.Background())
		case <-ticker.C:
			s.c.mu.Lock()
			s.c.onRTOLocked(time.Now())
			s.c.mu.Unlock()
		}
	}
}
