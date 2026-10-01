package rdt_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"sync"
	"testing"
	"time"

	"rdt/rdt"
	"rdt/sim"
)

func muxCfg() rdt.Config {
	return rdt.Config{
		MaxSegment:       128,
		SendBuffer:       1024,
		RecvBuffer:       2048,
		HandshakeTimeout: 100 * time.Millisecond,
		RTO:              40 * time.Millisecond,
		MaxStreams:       16,
	}
}

func muxPair(seed int64, loss, dup float64) sim.Pair {
	c := sim.Config{LossRate: loss, DupRate: dup, MinDelay: time.Millisecond, MaxDelay: 8 * time.Millisecond}
	return sim.NewPair(seed, c, c)
}

func muxPayload(id byte, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = id ^ byte(i*7+int(id)*13)
	}
	return b
}

// TestMultiStreamIndependent verifies several concurrent streams deliver
// exact byte streams under loss/reorder/duplication, each independently.
func TestMultiStreamIndependent(t *testing.T) {
	const N = 4
	pair := muxPair(2026, 0.25, 0.15)
	cfg := muxCfg()

	payloads := make([][]byte, N)
	for i := range payloads {
		payloads[i] = muxPayload(byte(i+1), 6000+i*137)
	}

	type recv struct {
		id  uint32
		buf []byte
		err error
	}
	got := make(chan recv, N*2)

	srvReady := make(chan *rdt.Conn, 1)
	go func() {
		srv, err := rdt.AcceptConn(context.Background(), pair.B, cfg)
		if err != nil {
			return
		}
		srvReady <- srv
		wg := sync.WaitGroup{}
		for i := 0; i < N; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				st, aerr := srv.AcceptStream()
				if aerr != nil {
					got <- recv{err: aerr}
					return
				}
				var buf bytes.Buffer
				if _, rerr := io.Copy(&buf, st); rerr != nil && rerr != io.EOF {
					got <- recv{id: st.ID, err: rerr}
					return
				}
				// Half close: server also sends back a reply while still
				// draining, and closes only its own write direction.
				reply := muxPayload(byte(st.ID), 500)
				st.Write(reply)
				st.CloseWrite()
				got <- recv{id: st.ID, buf: buf.Bytes()}
			}()
		}
		wg.Wait()
	}()

	cli, err := rdt.DialConn(context.Background(), pair.A, cfg)
	if err != nil {
		t.Fatal(err)
	}
	srv := <-srvReady
	_ = srv

	wg := sync.WaitGroup{}
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			st, oerr := cli.OpenStream()
			if oerr != nil {
				t.Errorf("open: %v", oerr)
				return
			}
			writeDone := make(chan error, 1)
			go func() {
				_, werr := st.Write(payloads[i])
				if werr == nil {
					werr = st.CloseWrite()
				}
				writeDone <- werr
			}()
			replyWant := muxPayload(byte(st.ID), 500)
			var gotReply bytes.Buffer
			if _, rerr := io.Copy(&gotReply, st); rerr != nil && rerr != io.EOF {
				t.Errorf("read reply: %v", rerr)
				return
			}
			if werr := <-writeDone; werr != nil {
				t.Errorf("write stream: %v", werr)
				return
			}
			if !bytes.Equal(gotReply.Bytes(), replyWant) {
				t.Errorf("stream %d reply mismatch", st.ID)
			}
		}(i)
	}
	wg.Wait()

	received := map[uint32][]byte{}
	deadline := time.After(20 * time.Second)
	for i := 0; i < N; i++ {
		select {
		case r := <-got:
			if r.err != nil {
				t.Fatalf("recv: %v", r.err)
			}
			received[r.id] = r.buf
		case <-deadline:
			t.Fatal("timeout waiting server streams")
		}
	}
	for i := 0; i < N; i++ {
		want := sha256.Sum256(payloads[i])
		// client streams are odd ids 3,5,7,9 (bootstrap id 1 skipped here)
		matched := false
		for _, b := range received {
			h := sha256.Sum256(b)
			if h == want {
				matched = true
			}
		}
		if !matched {
			t.Fatalf("payload %d not delivered intact", i)
		}
	}
}

// TestSlowStreamDoesNotBlockOthers: one stream is read very slowly (and
// then never read), while a second stream must still complete promptly
// thanks to per-stream windows and fair scheduling.
func TestSlowStreamDoesNotBlockOthers(t *testing.T) {
	pair := muxPair(77, 0.2, 0.1)
	cfg := muxCfg()

	done := make(chan struct{})
	go func() {
		srv, err := rdt.AcceptConn(context.Background(), pair.B, cfg)
		if err != nil {
			return
		}
		s1, _ := srv.AcceptStream()
		s2, _ := srv.AcceptStream()
		// Frames reorder, so accept order is not open order; dispatch by
		// the deterministic stream id (slow = 1, fast = 3).
		byID := map[uint32]*rdt.Stream{s1.ID: s1, s2.ID: s2}
		slow, fast := byID[1], byID[3]
		// Read the slow stream at a crawl with a tiny buffer.
		go func() {
			tmp := make([]byte, 1)
			for {
				n, err := slow.Read(tmp)
				if err != nil {
					return
				}
				_ = n
				time.Sleep(5 * time.Millisecond)
			}
		}()
		io.Copy(io.Discard, fast)
		fast.CloseWrite()
		close(done)
	}()

	cli, err := rdt.DialConn(context.Background(), pair.A, cfg)
	if err != nil {
		t.Fatal(err)
	}
	slow, _ := cli.OpenStream()
	fast, _ := cli.OpenStream()

	// Keep pouring data into the slow stream; it must not unboundedly
	// buffer: Write eventually blocks (flow control), checked separately.
	go func() {
		slow.Write(muxPayload(1, 20000))
		slow.CloseWrite()
	}()

	start := time.Now()
	if _, err := fast.Write(muxPayload(2, 12000)); err != nil {
		t.Fatalf("fast write: %v", err)
	}
	fast.CloseWrite()
	select {
	case <-done:
		t.Logf("fast stream finished in %v despite slow sibling", time.Since(start))
	case <-time.After(15 * time.Second):
		t.Fatal("fast stream starved by slow sibling")
	}
	// Abort the whole connection to unwind the still-running slow stream.
	_ = slow.Close()
	_ = cli.Close(context.Background())
}

// TestSlowStreamBoundedBuffers verifies a never-read stream cannot make
// the sender buffer grow past SendBuffer: after the window fills, Write
// blocks while a sibling stream keeps working.
func TestSlowStreamBoundedBuffers(t *testing.T) {
	pair := muxPair(88, 0.05, 0.0)
	cfg := muxCfg()

	accepted := make(chan *rdt.Stream, 2)
	go func() {
		srv, _ := rdt.AcceptConn(context.Background(), pair.B, cfg)
		for {
			st, err := srv.AcceptStream()
			if err != nil {
				return
			}
			accepted <- st
		}
	}()

	cli, err := rdt.DialConn(context.Background(), pair.A, cfg)
	if err != nil {
		t.Fatal(err)
	}
	st1, _ := cli.OpenStream()
	st2, _ := cli.OpenStream()

	// First writes admit both streams at the server.
	if _, err := st1.Write(make([]byte, 64)); err != nil {
		t.Fatal(err)
	}
	if _, err := st2.Write(make([]byte, 64)); err != nil {
		t.Fatal(err)
	}
	<-accepted
	<-accepted

	// Flood the never-read stream; it must stall under flow control
	// rather than buffering without bound.
	floodDone := make(chan struct{})
	go func() {
		_, _ = st1.Write(make([]byte, 1<<20))
		close(floodDone)
	}()
	select {
	case <-floodDone:
		t.Fatal("slow stream buffered unbounded data")
	case <-time.After(500 * time.Millisecond):
		// expected: still blocked by the bounded receive window
	}

	// Reset unblocks the flooded writer with an error and frees buffers;
	// the sibling stream is unaffected.
	if err := st1.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-floodDone:
	case <-time.After(3 * time.Second):
		t.Fatal("RST did not unwind the blocked writer")
	}
	if _, err := st2.Write(make([]byte, 64)); err != nil {
		t.Fatalf("sibling write after reset: %v", err)
	}
	st2.Close()
	cli.Close(context.Background())
}

// TestStreamCancelIsolation: resetting one stream unblocks its Read/Write
// and never disturbs delivery on a sibling.
func TestStreamCancelIsolation(t *testing.T) {
	pair := muxPair(99, 0.2, 0.1)
	cfg := muxCfg()
	type got struct {
		id  uint32
		buf []byte
	}
	res := make(chan got, 1)
	srvReady := make(chan *rdt.Conn, 1)
	go func() {
		srv, _ := rdt.AcceptConn(context.Background(), pair.B, cfg)
		srvReady <- srv
		s1, _ := srv.AcceptStream()
		s2, _ := srv.AcceptStream()
		byID := map[uint32]*rdt.Stream{s1.ID: s1, s2.ID: s2}
		victim, healthy := byID[1], byID[3]
		// Victim gets cancelled locally (peer will see RST).
		go func() {
			tmp := make([]byte, 16)
			victim.Read(tmp)
		}()
		var buf bytes.Buffer
		io.Copy(&buf, healthy)
		res <- got{id: healthy.ID, buf: buf.Bytes()}
	}()
	cli, _ := rdt.DialConn(context.Background(), pair.A, cfg)
	srv := <-srvReady
	_ = srv
	victim, _ := cli.OpenStream()
	healthy, _ := cli.OpenStream()

	payload := muxPayload(3, 9000)
	go func() {
		for {
			if _, err := victim.Write([]byte{1, 2, 3}); err != nil {
				return
			}
		}
	}()
	time.Sleep(50 * time.Millisecond)
	vCloseErr := victim.Close() // RST; sibling unaffected
	if vCloseErr != nil {
		t.Fatalf("close victim: %v", vCloseErr)
	}
	if _, err := healthy.Write(payload); err != nil {
		t.Fatalf("healthy write after cancel: %v", err)
	}
	healthy.CloseWrite()
	select {
	case g := <-res:
		if !bytes.Equal(g.buf, payload) {
			t.Fatalf("sibling corrupted after cancel: %d vs %d", len(g.buf), len(payload))
		}
	case <-time.After(15 * time.Second):
		t.Fatal("sibling did not complete after other stream cancel")
	}
	cli.Close(context.Background())
}

// TestHalfCloseAllowsReverseData: after CloseWrite on one direction, all
// data flowing in the opposite direction still arrives completely.
func TestHalfCloseAllowsReverseData(t *testing.T) {
	pair := muxPair(303, 0.25, 0.1)
	cfg := muxCfg()
	up := muxPayload(4, 4000)
	down := muxPayload(5, 15000)
	result := make(chan []byte, 1)
	go func() {
		srv, _ := rdt.AcceptConn(context.Background(), pair.B, cfg)
		st, _ := srv.AcceptStream()
		var upBuf bytes.Buffer
		io.Copy(&upBuf, st) // client half-closes first; we still read all up
		// Only now send the large reverse payload while the client is
		// still reading (its send direction is already closed).
		st.Write(down)
		st.CloseWrite()
		result <- upBuf.Bytes()
	}()
	cli, _ := rdt.DialConn(context.Background(), pair.A, cfg)
	st, _ := cli.OpenStream()
	st.Write(up)
	if err := st.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	var gotDown bytes.Buffer
	done := make(chan struct{})
	go func() { io.Copy(&gotDown, st); close(done) }()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("reverse data after half close stalled")
	}
	upGot := <-result
	if !bytes.Equal(upGot, up) {
		t.Fatal("upstream truncated")
	}
	if !bytes.Equal(gotDown.Bytes(), down) {
		t.Fatalf("downstream after half-close mismatch: %d", gotDown.Len())
	}
}

// TestStreamCloseAndWaitGraceful checks the explicit graceful API under
// loss and duplication: both directions drain completely.
func TestStreamCloseAndWaitGraceful(t *testing.T) {
	pair := muxPair(414, 0.25, 0.15)
	cfg := muxCfg()
	up := muxPayload(7, 5000)
	down := muxPayload(8, 7000)
	res := make(chan []byte, 1)
	go func() {
		srv, _ := rdt.AcceptConn(context.Background(), pair.B, cfg)
		st, _ := srv.AcceptStream()
		var buf bytes.Buffer
		io.Copy(&buf, st) // EOF after client FIN
		st.Write(down)
		if err := st.CloseAndWait(context.Background()); err != nil {
			t.Errorf("server close-and-wait: %v", err)
		}
		res <- buf.Bytes()
	}()
	cli, _ := rdt.DialConn(context.Background(), pair.A, cfg)
	st, _ := cli.OpenStream()
	st.Write(up)
	st.CloseWrite() // half-close our direction; keep reading the reply
	done := make(chan error, 1)
	go func() {
		var buf bytes.Buffer
		io.Copy(&buf, st)
		if !bytes.Equal(buf.Bytes(), down) {
			done <- io.ErrUnexpectedEOF
			return
		}
		done <- st.CloseAndWait(context.Background())
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("client: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("graceful stream close stalled")
	}
	if got := <-res; !bytes.Equal(got, up) {
		t.Fatal("server did not receive complete upstream")
	}
}
