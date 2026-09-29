package rdt

import (
	"context"
	"time"
)

// Client: send SYN(epoch) until SYNACK(same epoch) arrives.
// Server: wait for SYN, adopt its epoch, answer SYNACK (with retransmits
// driven by repeated SYNs from the client).
func (s *Session) handshakeClient(ctx context.Context) error {
	syn := Frame{Kind: KindSyn, Epoch: s.epoch, Window: uint64(s.cfg.RecvBuffer)}
	dl := time.NewTimer(s.cfg.HandshakeTimeout)
	defer dl.Stop()
	for {
		s.send(syn)
		select {
		case <-ctx.Done():
			return ErrHandshake
		case <-s.done:
			return ErrClosed
		case <-s.rxDone:
			return ErrHandshake
		case f, ok := <-s.rxFrames:
			if !ok {
				return ErrHandshake
			}
			if f.Kind == KindSynAck && f.Epoch == s.epoch {
				s.sndWin = f.Window
				return nil
			}
			// stale/stray frame: keep trying
		case <-dl.C:
			dl.Reset(s.cfg.HandshakeTimeout)
		}
	}
}

func (s *Session) handshakeServer(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ErrHandshake
		case <-s.done:
			return ErrClosed
		case <-s.rxDone:
			return ErrHandshake
		case f, ok := <-s.rxFrames:
			if !ok {
				return ErrHandshake
			}
			if f.Kind != KindSyn {
				continue // wait for a SYN
			}
			s.epoch = f.Epoch
			if f.Window > 0 {
				s.sndWin = f.Window
			}
			synack := Frame{Kind: KindSynAck, Window: uint64(s.cfg.RecvBuffer)}
			// Answer; repeated SYNs (lost SYNACK) trigger repeated answers.
			s.send(synack)
			return nil
		}
	}
}
