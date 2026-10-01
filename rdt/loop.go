package rdt

import (
	"io"
	"time"
)

func (s *Session) start() {
	s.wg.Add(1)
	go s.eventLoop()
}

func (s *Session) kick() { s.cond.Broadcast() }

func (s *Session) eventLoop() {
	defer s.wg.Done()
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
	for _, st := range s.streams {
		st.peerReset = true
	}
	s.kick()
	s.mu.Unlock()
}

func (s *Session) handleFrame(f Frame) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
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
		s.sendLocked(Frame{Kind: KindSynAck, Stream: defaultStreamID, Window: s.availWindowLocked(defaultStreamID)})
	case KindFinAck:
		s.onFinAck(f)
	case KindRst:
		s.onRst(f)
	}
}

func (s *Session) rcvUsed(st *streamState) uint64 {
	return uint64(len(st.rcvBuf)) + st.rcvHoldBytes
}

func (s *Session) sendAckLocked(id uint32, st *streamState) {
	s.sendLocked(Frame{Kind: KindAck, Stream: id, Ack: st.rcvNxt, Window: s.availWindowLocked(id)})
}

func (s *Session) onData(f Frame) {
	st := s.getOrCreatePeerStreamLocked(f.Stream)
	if st == nil || st.peerReset || st.readEOF || st.resetSent {
		return
	}
	n := f.length()
	if n == 0 {
		s.sendAckLocked(f.Stream, st)
		return
	}
	if f.Seq < st.rcvNxt {
		if f.Seq+n <= st.rcvNxt {
			s.sendAckLocked(f.Stream, st)
			return
		}
		f.Payload = f.Payload[st.rcvNxt-f.Seq:]
		f.Seq = st.rcvNxt
		n = f.length()
	}
	if f.Seq > st.rcvNxt {
		free := uint64(s.cfg.RecvBuffer) - s.rcvUsed(st)
		if n > free {
			n = free
		}
		if n == 0 {
			s.sendAckLocked(f.Stream, st)
			return
		}
		var added uint64
		st.rcvHold, added = insertHeld(st.rcvHold, f.Seq, f.Payload[:n])
		st.rcvHoldBytes += added
		s.sendAckLocked(f.Stream, st)
		return
	}
	data := f.Payload
	free := uint64(s.cfg.RecvBuffer) - s.rcvUsed(st)
	if uint64(len(data)) > free {
		data = data[:free]
	}
	st.rcvBuf = append(st.rcvBuf, data...)
	st.rcvNxt += uint64(len(data))
	s.mergeHold(st)
	s.sendAckLocked(f.Stream, st)
	s.kick()
}

func (s *Session) mergeHold(st *streamState) {
	for {
		for len(st.rcvHold) > 0 && st.rcvHold[0].end() <= st.rcvNxt {
			h0 := st.rcvHold[0]
			st.rcvHoldBytes -= uint64(len(h0.data))
			st.rcvHold = st.rcvHold[1:]
		}
		if len(st.rcvHold) == 0 || st.rcvHold[0].seq > st.rcvNxt || st.rcvHold[0].end() <= st.rcvNxt {
			return
		}
		h := st.rcvHold[0]
		st.rcvHold = st.rcvHold[1:]
		st.rcvHoldBytes -= uint64(len(h.data))
		seg := h.data[st.rcvNxt-h.seq:]
		free := uint64(s.cfg.RecvBuffer) - s.rcvUsed(st)
		if uint64(len(seg)) > free {
			seg = seg[:free]
		}
		st.rcvBuf = append(st.rcvBuf, seg...)
		st.rcvNxt += uint64(len(seg))
	}
}
