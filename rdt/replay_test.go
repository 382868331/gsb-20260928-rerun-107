package rdt_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"rdt/rdt"
	"rdt/sim"
)

// recordingTransport feeds a sim pair and counts delivered payload bytes
// at the receiver; a run produces (journal, payload hash).
type runResult struct {
	hash [32]byte
}

func runWithJournal(t *testing.T, seed int64, j *sim.Journal, replay *sim.Replay, payload []byte) [32]byte {
	t.Helper()
	cfg := rdt.Config{
		MaxSegment:       96,
		SendBuffer:       768,
		RecvBuffer:       1024,
		HandshakeTimeout: 80 * time.Millisecond,
		RTO:              40 * time.Millisecond,
	}
	fc := sim.Config{LossRate: 0.35, DupRate: 0.12, MinDelay: time.Millisecond, MaxDelay: 7 * time.Millisecond}
	var pair sim.Pair
	if replay != nil {
		pair = sim.NewPairOptions(seed, fc, fc, sim.Options{Journal: j, Replay: replay})
	} else {
		pair = sim.NewPairOptions(seed, fc, fc, sim.Options{Journal: j})
	}
	got := make(chan []byte, 1)
	go func() {
		srv, err := rdt.Accept(context.Background(), pair.B, cfg)
		if err != nil {
			got <- nil
			return
		}
		var buf bytes.Buffer
		io.Copy(&buf, srv)
		srv.Close(context.Background())
		got <- buf.Bytes()
	}()
	cli, err := rdt.Dial(context.Background(), pair.A, cfg)
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
			t.Fatalf("payload mismatch %d vs %d", len(b), len(payload))
		}
		return sha256.Sum256(b)
	case <-time.After(30 * time.Second):
		t.Fatal("transfer timed out")
		return [32]byte{}
	}
}

// TestFaultDecisionReplay records the exact loss/dup/delay decision
// script of a faulty run and replays it: results must be identical.
func TestFaultDecisionReplay(t *testing.T) {
	payload := makePayload(7000)
	j1 := &sim.Journal{}
	h1 := runWithJournal(t, 909, j1, nil, payload)
	if len(j1.Snapshot()) == 0 {
		t.Fatal("no fault decisions recorded")
	}
	// Replay the recorded script verbatim.
	h2 := runWithJournal(t, 909, nil, sim.NewReplay(j1.Snapshot()), payload)
	if h1 != h2 {
		t.Fatalf("replayed run diverged: %x vs %x", h1[:8], h2[:8])
	}
	// Duplicate decisions must actually appear under the configured rate.
	dups := 0
	for _, d := range j1.Snapshot() {
		if d.Dups > 0 {
			dups++
		}
	}
	if dups == 0 {
		t.Fatal("expected duplicated frames in the recorded script")
	}
}

// replayableConn is a Transport whose queues persist across Dial/Accept
// cycles (like a flapping real datagram socket), so stale frames from
// old epochs remain observable after reconnect.
type replayableConn struct {
	in  chan rdt.Frame
	out chan rdt.Frame
}

func newReplayablePair() (*replayableConn, *replayableConn) {
	a := &replayableConn{in: make(chan rdt.Frame, 4096), out: make(chan rdt.Frame, 4096)}
	b := &replayableConn{in: make(chan rdt.Frame, 4096), out: make(chan rdt.Frame, 4096)}
	go func() {
		for f := range a.out {
			select {
			case b.in <- f:
			default:
			}
		}
	}()
	go func() {
		for f := range b.out {
			select {
			case a.in <- f:
			default:
			}
		}
	}()
	return a, b
}

func (c *replayableConn) Send(f rdt.Frame) error {
	select {
	case c.out <- f:
	default:
	}
	return nil
}
func (c *replayableConn) Recv() (rdt.Frame, error) { return <-c.in, nil }
func (c *replayableConn) Close() error             { return nil }

// TestReconnectStaleFramesIgnored repeatedly closes and rebuilds with a
// small buffer, then injects old-epoch DATA/ACK/FIN: they must never
// count as progress for the new stream.
func TestReconnectStaleFramesIgnored(t *testing.T) {
	a, b := newReplayablePair()
	cfg := rdt.Config{
		MaxSegment: 32, SendBuffer: 128, RecvBuffer: 192,
		HandshakeTimeout: 80 * time.Millisecond, RTO: 30 * time.Millisecond,
	}
	ctx := context.Background()
	var epochCount int64

	for round := 0; round < 6; round++ {
		done := make(chan []byte, 1)
		go func() {
			srv, err := rdt.Accept(ctx, b, cfg)
			if err != nil {
				done <- nil
				return
			}
			var buf bytes.Buffer
			io.Copy(&buf, srv)
			srv.Close(context.Background())
			done <- buf.Bytes()
		}()
		cli, err := rdt.Dial(ctx, a, cfg)
		if err != nil {
			t.Fatalf("round %d dial: %v", round, err)
		}
		atomic.AddInt64(&epochCount, 1)

		// Inject stale frames that claim progress in impossible ranges and
		// in old/foreign epochs before and during the new transfer.
		for ep := uint64(1); ep <= 3; ep++ {
			a.out <- rdt.Frame{Kind: rdt.KindAck, Epoch: ep, Stream: 1, Ack: 1 << 40, Window: 1 << 30}
			b.out <- rdt.Frame{Kind: rdt.KindData, Epoch: ep, Stream: 1, Seq: 0, Payload: []byte("OLD-STALE-BYTES")}
			a.out <- rdt.Frame{Kind: rdt.KindFin, Epoch: ep, Stream: 1, Seq: 99999}
		}

		payload := makePayload(1500 + round*131)
		cli.Write(payload)
		if err := cli.Close(context.Background()); err != nil {
			t.Fatalf("round %d close: %v", round, err)
		}
		select {
		case got := <-done:
			if !bytes.Equal(got, payload) {
				t.Fatalf("round %d polluted: %d vs %d", round, len(got), len(payload))
			}
		case <-time.After(20 * time.Second):
			t.Fatalf("round %d stalled", round)
		}
	}
	if atomic.LoadInt64(&epochCount) != 6 {
		t.Fatal("expected 6 fresh incarnations")
	}
}
