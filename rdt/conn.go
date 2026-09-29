package rdt

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"time"
)

var (
	ErrClosed   = errors.New("rdt: session closed")
	ErrCanceled = errors.New("rdt: session canceled")
	ErrRejected = errors.New("rdt: connection reset by peer")
)

var errEOF = io.EOF

// Config tunes a session. Zero values are replaced with sane defaults.
type Config struct {
	MSS         int
	SendWindow  int
	MinRTO      time.Duration
	MaxRTO      time.Duration
	HandshakeTO time.Duration
}

func (c Config) withDefaults() Config {
	if c.MSS <= 0 {
		c.MSS = 512
	}
	if c.SendWindow <= 0 {
		c.SendWindow = 8
	}
	if c.MinRTO <= 0 {
		c.MinRTO = 15 * time.Millisecond
	}
	if c.MaxRTO <= 0 {
		c.MaxRTO = 250 * time.Millisecond
	}
	if c.HandshakeTO <= 0 {
		c.HandshakeTO = 50 * time.Millisecond
	}
	return c
}

// Conn is a reliable, ordered, at-most-once byte session.
type Conn struct {
	tr     BoundTransport
	local  Addr
	remote Addr
	epoch  uint64
	cfg    Config

	sndMu     sync.Mutex
	sndUna    uint64
	sndNxt    uint64
	segs      map[uint64]*outSeg
	finQueued bool
	finSeq    uint64
	finAcked  bool
	wndTokens chan struct{}
	rto       time.Duration
	estabOnce sync.Once
	estab     chan struct{}
	hskFrames chan Packet
	dataReady chan struct{}
	dataOnce  sync.Once

	rcvMu       sync.Mutex
	rcvNxt      uint64
	rcvCache    map[uint64][]byte
	rcvCacheN   int
	rcvCacheMax int
	readBuf     bytes.Buffer
	rcvGotFin   bool
	rcvFinSeq   uint64
	rcvFinAcked bool
	readErr     error
	rcvCond     *sync.Cond

	closeSig    chan struct{}
	writeClosed chan struct{}
	done        chan struct{}
	finAckSig   chan struct{}
	in          chan Frame
	onClose     func()
	once        sync.Once
	wg          sync.WaitGroup
}

type outSeg struct {
	seq     uint64
	data    []byte
	fin     bool
	sentAt  time.Time
	attempt int
}

func newEpoch() uint64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return uint64(time.Now().UnixNano()) | 1
	}
	return binary.BigEndian.Uint64(b[:]) | 1
}

func newConn(tr BoundTransport, remote Addr, epoch uint64, cfg Config, rcvNxt uint64) *Conn {
	cfg = cfg.withDefaults()
	c := &Conn{
		tr:          tr,
		local:       tr.Self,
		remote:      remote,
		epoch:       epoch,
		cfg:         cfg,
		sndUna:      1,
		sndNxt:      1,
		segs:        make(map[uint64]*outSeg),
		wndTokens:   make(chan struct{}, cfg.SendWindow),
		rto:         cfg.MinRTO,
		rcvNxt:      rcvNxt,
		rcvCache:    make(map[uint64][]byte),
		rcvCacheMax: cfg.MSS * cfg.SendWindow,
		closeSig:    make(chan struct{}),
		writeClosed: make(chan struct{}),
		done:        make(chan struct{}),
		finAckSig:   make(chan struct{}, 1),
		in:          make(chan Frame, 64),
		estab:       make(chan struct{}),
		hskFrames:   make(chan Packet, 16),
		dataReady:   make(chan struct{}),
	}
	for i := 0; i < cfg.SendWindow; i++ {
		c.wndTokens <- struct{}{}
	}
	c.rcvCond = sync.NewCond(&c.rcvMu)
	c.wg.Add(2)
	go c.recvLoop()
	go c.retransLoop()
	return c
}

func (c *Conn) send(p Packet) {
	p.Epoch = c.epoch
	c.tr.Send(Frame{From: c.local, To: c.remote, Pkt: p})
}

// localFrame feeds packets demuxed by the client pump or server listener.
func (c *Conn) localFrame(f Frame) {
	select {
	case c.in <- f:
	case <-c.closeSig:
	}
}

func (c *Conn) markEstablished() {
	c.estabOnce.Do(func() { close(c.estab) })
	c.markDataReady()
}

// markDataReady allows data/FIN reception before the application-level Accept
// returns. The listener uses it so data racing the final handshake ACK is not
// discarded. markEstablished remains the handshake-completion gate.
func (c *Conn) markDataReady() {
	c.dataOnce.Do(func() { close(c.dataReady) })
}

func (c *Conn) dataOpen() bool {
	select {
	case <-c.dataReady:
		return true
	default:
		return false
	}
}

func (c *Conn) recvLoop() {
	defer c.wg.Done()
	for {
		select {
		case <-c.closeSig:
			c.linger()
			return
		case f, ok := <-c.in:
			if !ok {
				c.fail(ErrClosed)
				return
			}
			c.handle(f.Pkt)
		}
	}
}

// linger keeps replying to the peer's retransmitted FIN/data for a short
// window after a graceful local close. Without this, a lost final ACK would
// leave the peer retransmitting FIN forever. It mirrors TCP's TIME_WAIT.
func (c *Conn) linger() {
	c.rcvMu.Lock()
	needLinger := c.rcvFinAcked
	c.rcvMu.Unlock()
	if !needLinger {
		return
	}
	d := 2 * time.Second
	deadline := time.NewTimer(d)
	defer deadline.Stop()
	hb := time.NewTicker(30 * time.Millisecond)
	defer hb.Stop()
	for {
		select {
		case f, ok := <-c.in:
			if !ok {
				return
			}
			p := f.Pkt
			if p.Epoch == c.epoch && (p.has(flagFIN) || len(p.Data) > 0) {
				c.sendAck()
			}
		case <-hb.C:
			// Actively re-assert the final ACK so a FIN the peer keeps
			// retransmitting is acknowledged even without a new trigger.
			c.sendAck()
		case <-deadline.C:
			return
		}
	}
}

func (c *Conn) handle(p Packet) {
	if p.Epoch != c.epoch {
		// From a stale or future connection: never touch this session's data.
		return
	}
	if p.has(flagRST) {
		select {
		case <-c.estab:
			c.fail(ErrRejected)
		default:
			select {
			case c.hskFrames <- p:
			default:
			}
		}
		return
	}
	if p.has(flagSYN) {
		select {
		case c.hskFrames <- p:
		default:
		}
		// After establishment this is a retransmitted SYN-ACK whose final
		// handshake ACK was lost; answer it without touching data state.
		select {
		case <-c.estab:
			c.send(Packet{Seq: 1, Ack: 1, Flags: flagACK})
		default:
		}
		return
	}
	// Before establishment, pure ACKs drive the handshake; data/FIN are
	// accepted only once data is open (immediately for a listener, after
	// the SYN-ACK for a dialer). This keeps spurious frames out of byte 1
	// while not dropping data that races the final handshake ACK.
	select {
	case <-c.estab:
	default:
		if p.has(flagACK) && len(p.Data) == 0 && !p.has(flagFIN) {
			c.handleAck(p.Ack)
			select {
			case c.hskFrames <- p:
			default:
			}
			return
		}
		if !c.dataOpen() {
			return
		}
	}
	if p.has(flagACK) {
		c.handleAck(p.Ack)
	}
	if len(p.Data) > 0 || p.has(flagFIN) {
		c.handleData(p)
	}
}

func (c *Conn) handleAck(ack uint64) {
	c.sndMu.Lock()
	advanced := 0
	if ack > c.sndUna {
		for seq, seg := range c.segs {
			end := seq + uint64(len(seg.data))
			if seg.fin {
				end++
			}
			if end <= ack {
				delete(c.segs, seq)
				advanced++
				if seg.fin {
					c.finAcked = true
				}
			}
		}
		if ack > c.sndUna {
			c.sndUna = ack
		}
	}
	if advanced > 0 {
		c.rto = c.cfg.MinRTO
	}
	for i := 0; i < advanced; i++ {
		select {
		case c.wndTokens <- struct{}{}:
		default:
		}
	}
	finAcked := c.finAcked
	c.sndMu.Unlock()
	if finAcked {
		select {
		case c.finAckSig <- struct{}{}:
		default:
		}
	}
}

// handleData delivers at most once: duplicates and retransmissions are absorbed
// by rcvNxt, gaps are cached in order, and the application only sees contiguous
// bytes starting from the first byte of the stream.
func (c *Conn) handleData(p Packet) {
	c.rcvMu.Lock()
	if c.readErr != nil {
		c.rcvMu.Unlock()
		c.sendAckLocked()
		return
	}
	if p.has(flagFIN) {
		c.rcvGotFin = true
		c.rcvFinSeq = p.Seq
	}
	if len(p.Data) > 0 {
		switch {
		case p.Seq+uint64(len(p.Data)) <= c.rcvNxt:
			// Fully duplicate packet.
		case p.Seq == c.rcvNxt:
			c.deliverLocked(p.Data)
			c.drainCacheLocked()
			c.checkFinLocked()
		case p.Seq > c.rcvNxt:
			gap := int(p.Seq - c.rcvNxt)
			if gap <= c.rcvCacheMax && c.rcvCacheN+len(p.Data) <= c.rcvCacheMax {
				if _, dup := c.rcvCache[p.Seq]; !dup {
					c.rcvCache[p.Seq] = append([]byte(nil), p.Data...)
					c.rcvCacheN += len(p.Data)
				}
			}
		}
	}
	c.checkFinLocked()
	c.rcvMu.Unlock()
	c.sendAck()
}

func (c *Conn) deliverLocked(data []byte) {
	c.readBuf.Write(data)
	c.rcvNxt += uint64(len(data))
	c.rcvCond.Broadcast()
}

func (c *Conn) drainCacheLocked() {
	for {
		data, ok := c.rcvCache[c.rcvNxt]
		if !ok {
			return
		}
		delete(c.rcvCache, c.rcvNxt)
		c.rcvCacheN -= len(data)
		c.deliverLocked(data)
	}
}

func (c *Conn) checkFinLocked() {
	if c.rcvGotFin && c.rcvFinSeq == c.rcvNxt && c.readBuf.Len() == 0 && c.readErr == nil {
		c.rcvFinAcked = true
		c.readErr = errEOF
		c.rcvCond.Broadcast()
	}
}

func (c *Conn) sendAck() {
	c.rcvMu.Lock()
	c.sendAckLocked()
	c.rcvMu.Unlock()
}

func (c *Conn) sendAckLocked() {
	ack := c.rcvNxt
	if c.rcvFinAcked {
		ack++
	}
	c.send(Packet{Flags: flagACK, Seq: c.sndNxtSnapshot(), Ack: ack})
}

func (c *Conn) sndNxtSnapshot() uint64 {
	c.sndMu.Lock()
	defer c.sndMu.Unlock()
	return c.sndNxt
}

func (c *Conn) fail(err error) {
	c.rcvMu.Lock()
	if c.readErr == nil {
		c.readErr = err
	}
	c.rcvCond.Broadcast()
	c.rcvMu.Unlock()
	c.shutdown(false)
}

// Read returns contiguous, deduplicated bytes. Duplicate or reordered packets
// never cause bytes to be delivered twice. Read returns io.EOF after the peer
// finishes cleanly.
func (c *Conn) Read(p []byte) (int, error) {
	c.rcvMu.Lock()
	defer c.rcvMu.Unlock()
	for c.readBuf.Len() == 0 {
		if c.readErr != nil {
			return 0, c.readErr
		}
		select {
		case <-c.closeSig:
			if c.readErr == nil {
				return 0, ErrClosed
			}
			return 0, c.readErr
		default:
		}
		c.rcvCond.Wait()
	}
	n, err := c.readBuf.Read(p)
	wasFinAcked := c.rcvFinAcked
	if c.readBuf.Len() == 0 {
		c.checkFinLocked()
	}
	if !wasFinAcked && c.rcvFinAcked {
		c.sendAckLocked()
	}
	return n, err
}

// Write sends the whole buffer reliably, blocking while the send window is
// full. The send window is bounded by Config.SendWindow packets.
func (c *Conn) Write(ctx context.Context, data []byte) (int, error) {
	select {
	case <-c.writeClosed:
		return 0, ErrClosed
	default:
	}
	written := 0
	for len(data) > 0 {
		if err := ctx.Err(); err != nil {
			return written, err
		}
		select {
		case <-c.closeSig:
			return written, ErrClosed
		default:
		}
		n := c.cfg.MSS
		if n > len(data) {
			n = len(data)
		}
		chunk := append([]byte(nil), data[:n]...)
		if !c.acquireWindow(ctx) {
			return written, ErrClosed
		}
		c.sndMu.Lock()
		if c.closedState() {
			c.sndMu.Unlock()
			select {
			case c.wndTokens <- struct{}{}:
			default:
			}
			return written, ErrClosed
		}
		seq := c.sndNxt
		c.sndNxt += uint64(n)
		seg := &outSeg{seq: seq, data: chunk, sentAt: time.Now(), attempt: 1}
		c.segs[seq] = seg
		c.sndMu.Unlock()
		c.send(Packet{Seq: seq, Data: chunk})
		data = data[n:]
		written += n
	}
	return written, nil
}

func (c *Conn) acquireWindow(ctx context.Context) bool {
	select {
	case <-c.wndTokens:
		return true
	case <-c.closeSig:
		return false
	case <-ctx.Done():
		return false
	}
}

func (c *Conn) closedState() bool {
	select {
	case <-c.closeSig:
		return true
	default:
		return false
	}
}

// retransLoop implements cumulative go-back-N style loss recovery: the oldest
// unacked segment drives the timer; on timeout every unacked segment is sent
// again. RTO backs off exponentially per failed attempt and resets on progress.
func (c *Conn) retransLoop() {
	defer c.wg.Done()
	for {
		select {
		case <-c.closeSig:
			return
		default:
		}
		c.sndMu.Lock()
		oldest, rto := c.oldestLocked()
		c.sndMu.Unlock()
		var timeout <-chan time.Time
		var timer *time.Timer
		if oldest != nil {
			// Timer for the oldest segment; its age is checked at fire time
			// so a delayed timer after an ACK never triggers a false retransmit.
		}
		timer = time.NewTimer(rto)
		timeout = timer.C
		select {
		case <-c.closeSig:
			timer.Stop()
			return
		case <-timeout:
			timer.Stop()
			if oldest == nil {
				continue
			}
			if time.Since(oldest.sentAt) >= rto {
				c.retransmitAll()
			}
		}
	}
}

func (c *Conn) oldestLocked() (*outSeg, time.Duration) {
	var oldest *outSeg
	for _, seg := range c.segs {
		if oldest == nil || seg.seq < oldest.seq {
			oldest = seg
		}
	}
	return oldest, c.rto
}

func (c *Conn) retransmitAll() {
	c.sndMu.Lock()
	now := time.Now()
	segments := make([]*outSeg, 0, len(c.segs))
	for _, seg := range c.segs {
		segments = append(segments, seg)
	}
	for _, seg := range segments {
		seg.attempt++
		seg.sentAt = now
		backoff := c.rto * 2
		if backoff > c.cfg.MaxRTO {
			backoff = c.cfg.MaxRTO
		}
		c.rto = backoff
	}
	c.sndMu.Unlock()
	for _, seg := range segments {
		p := Packet{Seq: seg.seq, Data: seg.data}
		if seg.fin {
			p.Flags = flagFIN
		}
		c.send(p)
	}
}

// queueFIN sends a FIN and records it in the retransmission window.
func (c *Conn) queueFIN() (uint64, bool) {
	c.sndMu.Lock()
	defer c.sndMu.Unlock()
	if c.finQueued {
		return c.finSeq, false
	}
	if c.sndNxt == 1 {
		// This side never sent data; an empty-direction close needs no FIN
		// on the wire. Mark it locally acknowledged so Close can complete.
		c.finQueued = true
		c.finSeq = c.sndNxt
		c.finAcked = true
		return c.finSeq, false
	}
	c.finQueued = true
	c.finSeq = c.sndNxt
	c.segs[c.finSeq] = &outSeg{seq: c.finSeq, fin: true, sentAt: time.Now(), attempt: 1}
	return c.finSeq, true
}

// CloseWrite half-closes the sending side: all buffered data plus a FIN are
// delivered and acknowledged, but the receiving side stays up. This is what a
// receiver calls once it has read EOF and has nothing to send back.
func (c *Conn) CloseWrite(ctx context.Context) error {
	close(c.writeClosed)
	seq, fresh := c.queueFIN()
	if fresh {
		c.send(Packet{Seq: seq, Flags: flagFIN})
	} else {
		c.sndMu.Lock()
		acked := c.finAcked
		c.sndMu.Unlock()
		if acked {
			return nil
		}
	}
	select {
	case <-c.finAckSig:
		return nil
	case <-c.closeSig:
		return ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close performs a full graceful close: this side's FIN is acknowledged, the
// peer's FIN has been read as EOF, then the connection lingers briefly so a
// lost final ACK is repaired by retransmitted FINs.
func (c *Conn) Close(ctx context.Context) error {
	select {
	case <-c.writeClosed:
	default:
		if err := c.CloseWrite(ctx); err != nil {
			return err
		}
	}
	// If the peer also closed its send direction, wait for its FIN to be
	// fully processed. A peer with an empty send direction sends no FIN, so
	// there is nothing further to wait for in the common one-way transfer.
	c.rcvMu.Lock()
	peerFin := c.rcvGotFin
	peerDone := c.rcvFinAcked
	c.rcvMu.Unlock()
	if peerFin && !peerDone {
		done := make(chan struct{})
		go func() {
			c.rcvMu.Lock()
			defer c.rcvMu.Unlock()
			for !c.rcvFinAcked && c.readErr == nil {
				c.rcvCond.Wait()
			}
			close(done)
		}()
		select {
		case <-done:
		case <-c.closeSig:
			return ErrClosed
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	c.shutdown(false)
	return c.waitDone(ctx)
}

// Cancel aborts the session immediately, sends a RST so the peer also stops,
// and waits for both sides' goroutines to finish.
func (c *Conn) Cancel(ctx context.Context) error {
	c.send(Packet{Flags: flagRST})
	c.shutdown(true)
	return c.waitDone(ctx)
}

func (c *Conn) waitDone(ctx context.Context) error {
	select {
	case <-c.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Conn) shutdown(force bool) {
	c.once.Do(func() {
		close(c.closeSig)
		c.rcvMu.Lock()
		if c.readErr == nil {
			c.readErr = ErrClosed
		}
		c.rcvCond.Broadcast()
		c.rcvMu.Unlock()
	})
	go func() {
		c.wg.Wait()
		if c.onClose != nil {
			c.onClose()
		}
		close(c.done)
	}()
}

// Done is closed when every goroutine of the session has exited.
func (c *Conn) Done() <-chan struct{} { return c.done }

// LocalAddr and RemoteAddr expose the endpoints.
func (c *Conn) LocalAddr() Addr  { return c.local }
func (c *Conn) RemoteAddr() Addr { return c.remote }
