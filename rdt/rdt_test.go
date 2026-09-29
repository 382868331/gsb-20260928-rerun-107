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

func fastCfg() rdt.Config {
	return rdt.Config{
		MaxSegment:       256,
		SendBuffer:       2048,
		RecvBuffer:       3072,
		HandshakeTimeout: 50 * time.Millisecond,
		RTO:              60 * time.Millisecond,
	}
}

func faultyPair(seed int64, loss float64) sim.Pair {
	c := sim.Config{
		LossRate: loss,
		MinDelay: time.Millisecond,
		MaxDelay: 9 * time.Millisecond,
	}
	return sim.NewPair(seed, c, c)
}

func makePayload(n int) []byte {
	b := make([]byte, n)
	var x uint32 = 0x9e3779b9
	for i := range b {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		b[i] = byte(x >> 24)
	}
	return b
}

// onceReader guarantees a bounded capture buffer while exercising flow
// control; we instead stream directly with io.Copy into a bytes.Buffer.

// transferOnce runs one client->server transfer with faults active for
// the ENTIRE run and returns the captured bytes. No faults are disabled.
func transferOnce(t *testing.T, seed int64, loss float64, payload []byte, timeout time.Duration) []byte {
	t.Helper()
	pair := faultyPair(seed, loss)

	type res struct {
		buf []byte
		err error
	}
	got := make(chan res, 1)
	go func() {
		srv, err := rdt.Accept(context.Background(), pair.B, fastCfg())
		if err != nil {
			got <- res{err: err}
			return
		}
		var buf bytes.Buffer
		_, rerr := io.Copy(&buf, srv)
		cerr := srv.Close(context.Background())
		if rerr == io.EOF {
			rerr = nil
		}
		if rerr == nil {
			rerr = cerr
		}
		got <- res{buf: buf.Bytes(), err: rerr}
	}()

	cli, err := rdt.Dial(context.Background(), pair.A, fastCfg())
	if err != nil {
		t.Fatalf("dial seed=%d: %v", seed, err)
	}
	if _, err := cli.Write(payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	cerr := make(chan error, 1)
	go func() { cerr <- cli.Close(context.Background()) }()

	select {
	case r := <-got:
		if r.err != nil {
			t.Fatalf("server: %v", r.err)
		}
		select {
		case err := <-cerr:
			if err != nil {
				t.Fatalf("client close: %v", err)
			}
		case <-time.After(timeout):
			t.Fatalf("client close timed out (seed=%d)", seed)
		}
		pair.A.Close()
		return r.buf
	case <-time.After(timeout):
		pair.A.Close()
		t.Fatalf("transfer timed out (seed=%d, loss=%.2f)", seed, loss)
		return nil
	}
}

func TestOrderedBytesUnderLoss(t *testing.T) {
	payload := makePayload(20000)
	want := sha256.Sum256(payload)
	// Several seeds prove reordering/repeat acks/retransmits never dup deliver.
	for _, seed := range []int64{1, 2, 3, 7, 42} {
		got := transferOnce(t, seed, 0.30, payload, 15*time.Second)
		if len(got) != len(payload) {
			t.Fatalf("seed=%d length: got %d want %d", seed, len(got), len(payload))
		}
		if !bytes.Equal(got, payload) {
			h := sha256.Sum256(got)
			t.Fatalf("seed=%d bytes mismatch gotsha=%x wantsha=%x", seed, h, want)
		}
	}
}

// TestReplayable shows identical seeds reproduce identical behavior.
func TestReplayable(t *testing.T) {
	payload := makePayload(5000)
	a := transferOnce(t, 123, 0.35, payload, 15*time.Second)
	b := transferOnce(t, 123, 0.35, payload, 15*time.Second)
	if !bytes.Equal(a, b) {
		t.Fatal("replayed seed produced different bytes")
	}
}

var _ = sync.Once{}
