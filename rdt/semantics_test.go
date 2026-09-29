package rdt_test

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"rdt/rdt"
	"rdt/sim"
)

// sessionConn is a Transport backed by independent queues so two sessions
// on the same logical link can be torn down and rebuilt with new epochs.
type memConn struct {
	in  chan rdt.Frame
	out chan rdt.Frame
}

func newMemPair() (*memConn, *memConn) {
	a := &memConn{in: make(chan rdt.Frame, 256), out: make(chan rdt.Frame, 256)}
	b := &memConn{in: make(chan rdt.Frame, 256), out: make(chan rdt.Frame, 256)}
	// a.out -> b.in ; b.out -> a.in
	go pipe(a.out, b.in)
	go pipe(b.out, a.in)
	return a, b
}

func pipe(src, dst chan rdt.Frame) {
	for f := range src {
		select {
		case dst <- f:
		default: // drop if receiver already closed
		}
	}
}

func (c *memConn) Send(f rdt.Frame) error {
	select {
	case c.out <- f:
		return nil
	default:
		return nil
	}
}

func (c *memConn) Recv() (rdt.Frame, error) {
	f, ok := <-c.in
	if !ok {
		return rdt.Frame{}, io.EOF
	}
	return f, nil
}

func (c *memConn) Close() error {
	// Intentionally do not close shared channels; sessions finish via done.
	return nil
}

// TestEpochIsolation proves frames from an old incarnation are dropped
// after a connection is rebuilt, so stale packets cannot pollute new data.
func TestEpochIsolation(t *testing.T) {
	a, b := newMemPair()
	ctx := context.Background()

	// First incarnation: send one small message, then tear both down.
	srvDone := make(chan struct{})
	var firstBuf bytes.Buffer
	go func() {
		srv, err := rdt.Accept(ctx, b, fastCfg())
		if err == nil {
			io.Copy(&firstBuf, srv)
			srv.Close(context.Background())
		}
		close(srvDone)
	}()
	cli, err := rdt.Dial(ctx, a, fastCfg())
	if err != nil {
		t.Fatal(err)
	}
	oldPayload := []byte("OLD-INCARNATION-DATA")
	cli.Write(oldPayload)
	if err := cli.Close(context.Background()); err != nil {
		t.Fatalf("old close: %v", err)
	}
	<-srvDone

	// Rebuild on the SAME transports. New client gets a fresh random epoch.
	// Queue stale old-epoch frames into both directions before rebuilding.
	a.out <- rdt.Frame{Kind: rdt.KindData, Epoch: 1, Seq: 0, Payload: []byte("STALE")}
	a.out <- rdt.Frame{Kind: rdt.KindFin, Epoch: 1, Seq: 999}
	b.out <- rdt.Frame{Kind: rdt.KindAck, Epoch: 2, Ack: 999, Window: 1 << 20}

	var newBuf bytes.Buffer
	srvDone2 := make(chan struct{})
	go func() {
		srv2, err := rdt.Accept(ctx, b, fastCfg())
		if err != nil {
			close(srvDone2)
			return
		}
		io.Copy(&newBuf, srv2)
		srv2.Close(context.Background())
		close(srvDone2)
	}()
	cli2, err := rdt.Dial(ctx, a, fastCfg())
	if err != nil {
		t.Fatal(err)
	}
	newPayload := []byte("NEW-CLEAN-SESSION-BYTES")
	cli2.Write(newPayload)
	if err := cli2.Close(context.Background()); err != nil {
		t.Fatalf("new close: %v", err)
	}
	<-srvDone2

	if !bytes.Equal(newBuf.Bytes(), newPayload) {
		t.Fatalf("new session polluted: got %q want %q", newBuf.Bytes(), newPayload)
	}
}

// TestCancellationWindsDown verifies both sides unblock and report an
// error/EOF when the active side cancels its Close context mid-transfer.
func TestCancellationWindsDown(t *testing.T) {
	pair := faultyPair(5, 0.35)
	got := make(chan error, 1)
	var n int
	go func() {
		srv, err := rdt.Accept(context.Background(), pair.B, fastCfg())
		if err != nil {
			got <- err
			return
		}
		buf := make([]byte, 256)
		for {
			k, rerr := srv.Read(buf)
			n += k
			if rerr != nil {
				got <- rerr
				return
			}
		}
	}()
	cli, err := rdt.Dial(context.Background(), pair.A, fastCfg())
	if err != nil {
		t.Fatal(err)
	}
	// Flood data, then cancel while much of it is still outstanding.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 2000; i++ {
			if _, werr := cli.Write(make([]byte, 100)); werr != nil {
				return
			}
		}
	}()
	time.Sleep(100 * time.Millisecond)
	cctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err = cli.Close(cctx)
	if err == nil {
		t.Log("clean close beat cancellation (allowed)")
	} else {
		if err != context.Canceled && err != context.DeadlineExceeded {
			t.Fatalf("unexpected close err %v", err)
		}
	}
	<-done
	select {
	case <-got:
		// server read loop unwound
	case <-time.After(2 * time.Second):
		t.Fatal("server did not wind down after cancellation")
	}
}

// TestNoDuplicateDelivery uses high loss/reorder so retransmits and
// duplicate acks are frequent; exact byte equality proves no duplicate
// or reordering reaches the application.
func TestNoDuplicateDelivery(t *testing.T) {
	payload := makePayload(30000)
	c := sim.Config{LossRate: 0.45, MinDelay: time.Millisecond, MaxDelay: 12 * time.Millisecond}
	pair := sim.NewPair(777, c, c)
	got := make(chan []byte, 1)
	go func() {
		srv, err := rdt.Accept(context.Background(), pair.B, fastCfg())
		if err != nil {
			got <- nil
			return
		}
		var buf bytes.Buffer
		io.Copy(&buf, srv)
		srv.Close(context.Background())
		got <- buf.Bytes()
	}()
	cli, err := rdt.Dial(context.Background(), pair.A, fastCfg())
	if err != nil {
		t.Fatal(err)
	}
	cli.Write(payload)
	if err := cli.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case b := <-got:
		if !bytes.Equal(b, payload) {
			t.Fatalf("delivery mismatch: got %d bytes want %d", len(b), len(payload))
		}
	case <-time.After(30 * time.Second):
		t.Fatal("timeout under heavy loss")
	}
}
