package rdt

import (
	"context"
	"time"
)

// Client: send SYN(epoch) until SYNACK(same epoch) arrives.
// Server: wait for SYN, adopt its epoch, answer SYNACK; repeated SYNs
// after a lost SYNACK receive repeated answers via the event loop.
func (c *Conn) handshakeClient(ctx context.Context) error {
	syn := Frame{Kind: KindSyn, Epoch: c.epoch, Window: uint64(c.cfg.RecvBuffer)}
	dl := time.NewTimer(c.cfg.HandshakeTimeout)
	defer dl.Stop()
	for {
		c.send(syn)
		select {
		case <-ctx.Done():
			return ErrHandshake
		case <-c.done:
			return ErrClosed
		case <-c.rxDone:
			return ErrHandshake
		case f, ok := <-c.rxFrames:
			if !ok {
				return ErrHandshake
			}
			if f.Kind == KindSynAck && f.Epoch == c.epoch {
				c.onHandshakeWindow(f.Window)
				return nil
			}
		case <-dl.C:
			dl.Reset(c.cfg.HandshakeTimeout)
		}
	}
}

func (c *Conn) handshakeServer(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ErrHandshake
		case <-c.done:
			return ErrClosed
		case <-c.rxDone:
			return ErrHandshake
		case f, ok := <-c.rxFrames:
			if !ok {
				return ErrHandshake
			}
			if f.Kind != KindSyn {
				continue
			}
			c.epoch = f.Epoch
			c.onHandshakeWindow(f.Window)
			c.send(Frame{Kind: KindSynAck, Window: uint64(c.cfg.RecvBuffer)})
			return nil
		}
	}
}
