package rdt

import (
	"io"
	"sort"
	"sync"
	"time"
)

type streamState uint8

const (
	stOpen    streamState = iota // data flows both ways
	stFinWait                    // local FIN sent, awaiting FinAck
	stClosing                    // FinAck received; only draining inbound
	stClosed                     // fully finished or reset
)

// held is an out-of-order received byte interval for one stream.
type held struct {
	seq  uint64
	data []byte
}

func (h held) end() uint64 { return h.seq + uint64(len(h.data)) }

// Stream is one independent bidirectional byte stream inside a Conn.
type Stream struct {
	c  *Conn
	ID uint32

	mu    sync.Mutex
	cond  *sync.Cond
	state streamState
	reset bool // peer sent RST or local side cancelled

	// send side
	sndUna    uint64
	sndNxt    uint64
	sndWin    uint64
	segs      map[uint64]*outSeg // in flight, keyed by Seq
	queued    []byte
	finQueued bool
	finSent   bool
	finSeq    uint64 // Seq used by our FIN on our send direction

	// receive side
	rcvNxt        uint64
	rcvBuf        []byte
	rcvHold       []held
	rcvHoldBytes  uint64
	peerFin       bool
	readEOF       bool
	finQuietUntil time.Time // keep answering retransmitted peer FINs
}

type outSeg struct {
	seq    uint64
	data   []byte
	sentAt time.Time
}

func newStream(c *Conn, id uint32) *Stream {
	win := c.hshakeWin
	if win == 0 {
		win = uint64(c.cfg.RecvBuffer)
	}
	s := &Stream{
		c:      c,
		ID:     id,
		state:  stOpen,
		sndWin: win,
		segs:   make(map[uint64]*outSeg),
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

func (s *Stream) outstandingLocked() uint64 {
	n := uint64(len(s.queued))
	for _, seg := range s.segs {
		n += uint64(len(seg.data))
	}
	return n
}

func (s *Stream) rcvUsedLocked() uint64 {
	return uint64(len(s.rcvBuf)) + s.rcvHoldBytes
}

func (s *Stream) availWindowLocked() uint64 {
	return uint64(s.c.cfg.RecvBuffer) - s.rcvUsedLocked()
}

func (s *Stream) sendAckLocked() {
	s.c.sendLocked(Frame{
		Kind:   KindAck,
		Stream: s.ID,
		Ack:    s.rcvNxt,
		Window: s.availWindowLocked(),
	})
}

// ---- receive -----------------------------------------------------------

func (s *Stream) onDataLocked(f Frame) {
	n := uint64(len(f.Payload))
	if n == 0 {
		s.sendAckLocked() // zero-window / duplicate probe
		return
	}
	// Retransmitted segments can straddle the delivery frontier.
	if f.Seq < s.rcvNxt {
		if f.Seq+n <= s.rcvNxt {
			s.sendAckLocked()
			return
		}
		f.Payload = f.Payload[s.rcvNxt-f.Seq:]
		f.Seq = s.rcvNxt
		n = uint64(len(f.Payload))
	}
	if f.Seq > s.rcvNxt {
		free := s.availWindowLocked()
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
	off := s.rcvNxt - f.Seq
	data := f.Payload[off:]
	free := s.availWindowLocked()
	if uint64(len(data)) > free {
		data = data[:free]
	}
	s.rcvBuf = append(s.rcvBuf, data...)
	s.rcvNxt += uint64(len(data))
	s.mergeHoldLocked()
	s.sendAckLocked()
	s.cond.Broadcast()
}

func (s *Stream) mergeHoldLocked() {
	for {
		for len(s.rcvHold) > 0 && s.rcvHold[0].end() <= s.rcvNxt {
			h0 := s.rcvHold[0]
			s.rcvHoldBytes -= uint64(len(h0.data))
			s.rcvHold = s.rcvHold[1:]
		}
		if len(s.rcvHold) == 0 || s.rcvHold[0].seq > s.rcvNxt {
			return
		}
		h := s.rcvHold[0]
		s.rcvHold = s.rcvHold[1:]
		s.rcvHoldBytes -= uint64(len(h.data))
		seg := h.data[s.rcvNxt-h.seq:]
		free := s.availWindowLocked()
		if uint64(len(seg)) > free {
			seg = seg[:free]
		}
		s.rcvBuf = append(s.rcvBuf, seg...)
		s.rcvNxt += uint64(len(seg))
	}
}

func insertHeld(hold []held, seq uint64, data []byte) ([]held, uint64) {
	if len(data) == 0 {
		return hold, 0
	}
	before := holdBytes(hold)
	hold = append(hold, held{seq: seq, data: append([]byte(nil), data...)})
	sort.SliceStable(hold, func(i, j int) bool { return hold[i].seq < hold[j].seq })

	out := make([]held, 0, len(hold))
	for _, cur := range hold {
		if len(out) == 0 || cur.seq > out[len(out)-1].end() {
			out = append(out, cur)
			continue
		}
		last := &out[len(out)-1]
		if cur.end() <= last.end() {
			continue
		}
		off := last.end() - cur.seq
		if off > uint64(len(cur.data)) {
			off = uint64(len(cur.data))
		}
		last.data = append(last.data, cur.data[off:]...)
	}
	return out, holdBytes(out) - before
}

func holdBytes(hold []held) uint64 {
	v := uint64(0)
	for _, h := range hold {
		v += uint64(len(h.data))
	}
	return v
}

// markEOF closes the read side once a FIN and all preceding data arrived.
func (s *Stream) markEOF() {
	s.readEOF = true
	s.cond.Broadcast()
}

// Read implements io.Reader on one stream.
func (s *Stream) Read(p []byte) (int, error) {
	s.mu.Lock()
	for len(s.rcvBuf) == 0 && !s.readEOF && !s.reset {
		s.cond.Wait()
	}
	if len(s.rcvBuf) > 0 {
		n := copy(p, s.rcvBuf)
		s.rcvBuf = s.rcvBuf[n:]
		s.sendAckLocked() // advertise freed space even with no new DATA
		s.mu.Unlock()
		return n, nil
	}
	if s.reset {
		s.mu.Unlock()
		return 0, ErrStreamClosed
	}
	s.mu.Unlock()
	return 0, io.EOF
}
