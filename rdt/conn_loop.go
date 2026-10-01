package rdt

import (
	"io"
	"sort"
	"time"
)

func (c *Conn) start() {
	c.wg.Add(1)
	go c.eventLoop()
}

func (c *Conn) eventLoop() {
	defer c.wg.Done()
	tick := time.NewTicker(c.cfg.RTO / 4)
	defer tick.Stop()
	for {
		c.mu.Lock()
		st := c.state
		c.mu.Unlock()
		if st == connClosed {
			return
		}
		select {
		case <-c.done:
			return
		case <-c.rxDone:
			c.failTransport(io.ErrUnexpectedEOF)
			return
		case f := <-c.rxFrames:
			c.handleFrame(f)
		case <-tick.C:
			c.mu.Lock()
			c.onRTOLocked(time.Now())
			c.mu.Unlock()
		}
	}
}

func (c *Conn) failTransport(err error) {
	c.mu.Lock()
	c.state = connClosed
	c.closed = true
	c.recvErr = err
	for _, st := range c.streams {
		st.resetLocked()
	}
	c.cond.Broadcast()
	c.mu.Unlock()
}

func (c *Conn) handleFrame(f Frame) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	// Epoch isolation: frames from an earlier incarnation carry no valid
	// progress, including old acks and old data.
	if f.Epoch != c.epoch {
		return
	}
	switch f.Kind {
	case KindSyn:
		// Retransmitted SYN after a lost SYNACK: answer again.
		c.sendLocked(Frame{Kind: KindSynAck, Window: uint64(c.cfg.RecvBuffer)})
		return
	case KindSynAck:
		return
	}

	// A fully-finished stream only answers repeats with RST so the peer
	// stops retransmitting; its old bytes/acks can never count again.
	if _, dead := c.tombstones[f.Stream]; dead {
		// FinAck for a cleanly finished stream is the last expected word;
		// only data/fin repeats from a confused peer earn an RST.
		if f.Kind == KindData || f.Kind == KindFin {
			c.sendLocked(Frame{Kind: KindRst, Stream: f.Stream})
		}
		return
	}
	st := c.streams[f.Stream]
	if st == nil {
		st = c.admitPeerStreamLocked(f.Stream)
		if st == nil {
			return
		}
	}

	switch f.Kind {
	case KindData:
		c.onStreamData(st, f)
	case KindAck:
		c.onStreamAck(st, f)
	case KindFin:
		c.onStreamFin(st, f)
	case KindFinAck:
		c.onStreamFinAck(st, f)
	case KindRst:
		c.onStreamRst(st)
	}
}

// admitPeerStreamLocked creates an inbound stream. Odd ids belong to the
// client side, even ids to the server side; a parity matching the local
// side cannot legitimately be opened by the peer.
func (c *Conn) admitPeerStreamLocked(id uint32) *Stream {
	if id == 0 {
		return nil
	}
	localParity := uint32(1)
	if !c.isClient {
		localParity = 2
	}
	_ = localParity
	if (c.isClient && id%2 != 0) || (!c.isClient && id%2 != 1) {
		return nil // same parity as our allocations: never peer-initiated
	}
	if len(c.streams)-len(c.tombstones) >= c.cfg.MaxStreams {
		c.sendLocked(Frame{Kind: KindRst, Stream: id})
		return nil
	}
	st := newStream(c, id)
	c.streams[id] = st
	c.accept = append(c.accept, st)
	c.cond.Broadcast()
	return st
}

func (c *Conn) onStreamData(st *Stream, f Frame) {
	st.mu.Lock()
	if st.reset {
		st.mu.Unlock()
		c.sendLocked(Frame{Kind: KindRst, Stream: st.ID})
		return
	}
	st.onDataLocked(f)
	st.mu.Unlock()
}

func (c *Conn) onStreamAck(st *Stream, f Frame) {
	st.mu.Lock()
	c.advanceAckLocked(st, f.Ack, f.Window)
	st.mu.Unlock()
}

func (c *Conn) onStreamFin(st *Stream, f Frame) {
	st.mu.Lock()
	defer st.mu.Unlock()
	c.advanceAckLocked(st, f.Ack, f.Window)
	// FIN belongs to the peer's send sequence. It is consumable only once
	// every preceding byte in that direction has been delivered; rcvNxt
	// tracks exactly that frontier.
	if !st.peerFin && f.Seq == st.rcvNxt {
		st.peerFin = true
		st.rcvNxt = f.Seq + 1
		st.markEOF()
		st.maybeFinishLocked()
	}
	// FinAck.Ack acknowledges the peer FIN with its own send seq + 1, so a
	// bidirectional close carries distinct acks (e.g. 1 and 6).
	c.sendLocked(Frame{Kind: KindFinAck, Stream: st.ID, Ack: f.Seq + 1, Window: st.availWindowLocked()})
	st.finQuietUntil = time.Now().Add(c.cfg.CloseGrace)
}

func (c *Conn) onStreamFinAck(st *Stream, f Frame) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.sndWin = f.Window
	if st.finSent && f.Ack == st.finSeq+1 && st.state == stFinWait {
		st.state = stClosing
		st.cond.Broadcast()
		st.maybeFinishLocked()
	}
}

func (c *Conn) onStreamRst(st *Stream) {
	st.resetLocked()
	c.forgetStreamLocked(st)
}

// advanceAckLocked applies a cumulative ack and the peer window for one
// stream, then schedules that stream's queued data.
func (c *Conn) advanceAckLocked(s *Stream, ack, win uint64) {
	prevWin := s.sndWin
	s.sndWin = win
	delete(c.probeDead, s.ID)
	if win == 0 && (len(s.queued) > 0 || s.finQueued) {
		if _, ok := c.probeDead[s.ID]; !ok {
			c.probeDead[s.ID] = time.Now().Add(c.cfg.RTO)
		}
	}
	if ack > s.sndUna && ack <= s.sndNxt+1 {
		s.sndUna = ack
		for seq, seg := range s.segs {
			if seq+uint64(len(seg.data)) <= s.sndUna {
				delete(s.segs, seq)
			}
		}
		s.cond.Broadcast()
	}
	s.pumpLocked(-1)
	if win > prevWin {
		s.cond.Broadcast()
	}
}

// ---- fairness scheduler -----------------------------------------------

// scheduleLocked services every stream that can send, rotating a
// round-robin cursor each pass. Every ready stream emits at most one
// segment per round, so N active streams share wire turns roughly 1/N
// regardless of how much data each queued; a zero-window stream skips.
func (c *Conn) scheduleLocked() {
	ids := make([]uint32, 0, len(c.streams))
	for id := range c.streams {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	i := 0
	for i < len(ids) && ids[i] < c.rrHint {
		i++
	}
	order := append(ids[i:], ids[:i]...)
	progressed := false
	for _, id := range order {
		st := c.streams[id]
		if st == nil {
			continue
		}
		st.mu.Lock()
		if !st.reset && st.pumpLocked(1) > 0 {
			progressed = true
		}
		st.mu.Unlock()
	}
	if progressed {
		c.rrHint++
	}
}

// ---- retransmission / probes ------------------------------------------

func (c *Conn) onRTOLocked(now time.Time) {
	for id, dead := range c.probeDead {
		if !now.Before(dead) {
			if st := c.streams[id]; st != nil {
				st.mu.Lock()
				probeSeq := uint64(0)
				if st.sndUna > 0 {
					probeSeq = st.sndUna - 1
				}
				c.sendLocked(Frame{Kind: KindData, Stream: id, Seq: probeSeq, Payload: []byte{0}})
				c.probeDead[id] = now.Add(c.cfg.RTO)
				st.mu.Unlock()
			} else {
				delete(c.probeDead, id)
			}
		}
	}
	for _, st := range c.streams {
		st.mu.Lock()
		if st.reset {
			st.mu.Unlock()
			continue
		}
		for _, seg := range st.segs {
			if now.Sub(seg.sentAt) >= c.cfg.RTO {
				seg.sentAt = now
				c.sendLocked(Frame{Kind: KindData, Stream: st.ID, Seq: seg.seq, Payload: seg.data})
			}
		}
		if st.finSent {
			c.sendLocked(Frame{Kind: KindFin, Stream: st.ID, Seq: st.finSeq, Ack: st.rcvNxt, Window: st.availWindowLocked()})
		}
		if st.peerFin && !now.Before(st.finQuietUntil) {
			// Re-answer the peer's FIN until its FinAck quiets its close loop.
			c.sendLocked(Frame{Kind: KindFinAck, Stream: st.ID, Ack: st.rcvNxt, Window: st.availWindowLocked()})
			st.finQuietUntil = now.Add(c.cfg.RTO)
		}
		st.mu.Unlock()
	}
	c.scheduleLocked()
}
