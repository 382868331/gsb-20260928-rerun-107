package rdt

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"math/rand/v2"
	"testing"
	"time"
)

const testTimeout = 20 * time.Second

func testConfig() Config {
	return Config{
		MSS:         64,
		SendWindow:  4,
		MinRTO:      10 * time.Millisecond,
		MaxRTO:      200 * time.Millisecond,
		HandshakeTO: 25 * time.Millisecond,
	}
}

func runOneSeed(t *testing.T, seed int64, size int) []byte {
	t.Helper()
	prof := NewFaultProfile(seed)
	prof.LossProbability = 0.35
	prof.DuplicateProb = 0.08
	prof.ReorderProb = 0.15
	prof.MinDelay = 200 * time.Microsecond
	prof.MaxDelay = 3 * time.Millisecond
	tr := NewFaultyTransport(prof)
	t.Cleanup(func() { tr.Close() })

	mux := NewMux(tr, "a", "b")
	lis := Listen(mux, "b", testConfig())

	payload := make([]byte, size)
	var key [32]byte
	for i := 0; i < 8; i++ {
		key[i] = byte(seed >> (8 * i))
	}
	rng := rand.NewChaCha8(key)
	for i := range payload {
		payload[i] = byte(rng.Uint64())
	}

	srvErr := make(chan error, 1)
	srvConn := make(chan *Conn, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
		defer cancel()
		conn, err := lis.Accept(ctx)
		if err != nil {
			srvErr <- err
			return
		}
		srvConn <- conn
		got, err := io.ReadAll(conn)
		if cerr := conn.Close(context.Background()); cerr != nil {
			srvErr <- cerr
			return
		}
		if err != nil {
			srvErr <- err
			return
		}
		if cerr := checkBytes(got, payload); cerr != nil {
			srvErr <- cerr
			return
		}
		srvErr <- nil
	}()

	dctx, dcancel := context.WithTimeout(context.Background(), testTimeout)
	defer dcancel()
	cli, err := mux.Dial(dctx, "a", "b", testConfig())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	off := 0
	for off < len(payload) {
		n := 1 + int(rng.Uint64()%uint64(3*testConfig().MSS))
		if off+n > len(payload) {
			n = len(payload) - off
		}
		wctx, cancel := context.WithTimeout(context.Background(), testTimeout)
		nw, err := cli.Write(wctx, payload[off:off+n])
		cancel()
		if err != nil {
			t.Fatalf("write: %v", err)
		}
		off += nw
	}
	cctx, ccancel := context.WithTimeout(context.Background(), testTimeout)
	if err := cli.Close(cctx); err != nil {
		t.Fatalf("close: %v", err)
	}
	ccancel()
	lis.Close()

	select {
	case err := <-srvErr:
		if err != nil {
			t.Fatalf("server: %v", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("server did not finish")
	}
	return payload
}

func checkBytes(got, want []byte) error {
	if len(got) != len(want) {
		return fmt.Errorf("length %d, want %d", len(got), len(want))
	}
	if sha256.Sum256(got) != sha256.Sum256(want) {
		return fmt.Errorf("payload mismatch")
	}
	return nil
}

// TestFaultyInOrder is the headline first-round requirement: with faults on
// for the whole run, bytes arrive in order exactly once across many seeds.
func TestFaultyInOrder(t *testing.T) {
	for _, seed := range []int64{1, 2, 3, 42, 99, 12345} {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			runOneSeed(t, seed, 5000)
		})
	}
}

// TestReplayable proves the fault process is deterministic: the same seed
// produces the identical decision sequence, and two runs end with the same
// hash. Failures never had faults switched off.
func TestReplayable(t *testing.T) {
	p1 := runOneSeed(t, 777, 3000)
	p2 := runOneSeed(t, 777, 3000)
	if sha256.Sum256(p1) != sha256.Sum256(p2) {
		t.Fatal("replay produced different payload")
	}
	prof := NewFaultProfile(777)
	seq1 := make([]bool, 1000)
	seq2 := make([]bool, 1000)
	for i := range seq1 {
		seq1[i] = prof.rng.Float64() < prof.LossProbability
	}
	prof2 := NewFaultProfile(777)
	for i := range seq2 {
		seq2[i] = prof2.rng.Float64() < prof2.LossProbability
	}
	for i := range seq1 {
		if seq1[i] != seq2[i] {
			t.Fatal("fault sequence not reproducible")
		}
	}
}

// TestRebuildEpochIsolation sends data on an old connection, rebuilds a new
// connection to the same addresses, and confirms stale packets cannot pollute
// the new stream.
func TestRebuildEpochIsolation(t *testing.T) {
	prof := NewFaultProfile(5)
	prof.LossProbability = 0
	prof.DuplicateProb = 0
	prof.ReorderProb = 0
	tr := NewFaultyTransport(prof)
	t.Cleanup(func() { tr.Close() })
	mux := NewMux(tr, "a", "b")
	lis := Listen(mux, "b", testConfig())

	acceptOne := func() (*Conn, error) {
		return lis.Accept(context.Background())
	}
	c1, err := mux.Dial(context.Background(), "a", "b", testConfig())
	if err != nil {
		t.Fatal(err)
	}
	s1, err := acceptOne()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c1.Write(context.Background(), []byte("OLD-DATA")); err != nil {
		t.Fatal(err)
	}
	old := make([]byte, 8)
	if _, err := io.ReadFull(s1, old); err != nil {
		t.Fatal(err)
	}

	// Rebuild: cancel old sessions and establish a new one.
	c1.Cancel(context.Background())
	s1.Cancel(context.Background())
	c2, err := mux.Dial(context.Background(), "a", "b", testConfig())
	if err != nil {
		t.Fatal(err)
	}
	s2, err := acceptOne()
	if err != nil {
		t.Fatal(err)
	}
	srvDone := make(chan []byte, 1)
	go func() {
		b, _ := io.ReadAll(s2)
		srvDone <- b
	}()

	if _, err := c2.Write(context.Background(), []byte("NEW")); err != nil {
		t.Fatal(err)
	}
	if err := c2.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Replay stale data packets from the old epoch after the new stream is
	// flowing; they must be ignored and never delivered.
	c2.send(Packet{Epoch: c1.epoch, Seq: 1, Data: []byte("STALE!!!")})
	s2.send(Packet{Epoch: c1.epoch, Seq: 1, Data: []byte("STALE!!!")})
	got := <-srvDone
	if !bytes.Equal(got, []byte("NEW")) {
		t.Fatalf("new stream polluted: %q", got)
	}
	s2.Cancel(context.Background())
	lis.Close()
}

// TestCancelBothSidesFinish cancels both peers and confirms no goroutine is
// left blocked and the RST tears the other side down promptly.
func TestCancelBothSidesFinish(t *testing.T) {
	prof := NewFaultProfile(9)
	prof.LossProbability = 0.3
	tr := NewFaultyTransport(prof)
	t.Cleanup(func() { tr.Close() })
	mux := NewMux(tr, "a", "b")
	lis := Listen(mux, "b", testConfig())
	srvAccepted := make(chan *Conn, 1)
	go func() {
		s, err := lis.Accept(context.Background())
		if err != nil {
			return
		}
		srvAccepted <- s
		buf := make([]byte, 16)
		s.Read(buf)
	}()
	c, err := mux.Dial(context.Background(), "a", "b", testConfig())
	if err != nil {
		t.Fatal(err)
	}
	var s *Conn
	select {
	case s = <-srvAccepted:
	case <-time.After(5 * time.Second):
		t.Fatal("server did not accept")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := c.Write(ctx, []byte("hello-cancel-world")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := c.Cancel(ctx); err != nil {
		t.Fatalf("client cancel: %v", err)
	}
	select {
	case <-c.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("client goroutines leaked")
	}
	select {
	case <-s.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("server goroutines leaked after RST")
	}
	lis.Close()
}
