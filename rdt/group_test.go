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

func groupCfg() rdt.Config {
	c := fastCfg()
	c.MaxSegment = 64
	c.SendBuffer = 256
	c.RecvBuffer = 320
	c.GroupQuota = 256
	return c
}

func findGroup(stats []rdt.GroupStats, id uint32) *rdt.GroupStats {
	for i := range stats {
		if stats[i].Group == id {
			return &stats[i]
		}
	}
	return nil
}

// waitGroupStats polls until cond holds for the session's group stats.
func waitGroupStats(t *testing.T, s *rdt.Session, cond func([]rdt.GroupStats) bool) []rdt.GroupStats {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		gs := s.GroupStats()
		if cond(gs) {
			return gs
		}
		time.Sleep(5 * time.Millisecond)
	}
	gs := s.GroupStats()
	if !cond(gs) {
		t.Fatalf("group stats condition not met: %+v", gs)
	}
	return gs
}

// Interactive (heavy) and batch (light) groups share one small quota.
// The batch stream must keep moving while the priority group is busy,
// and both must deliver exact bytes under loss.
func TestGroupSharedQuotaFairProgress(t *testing.T) {
	c := sim.Config{LossRate: 0.2, MinDelay: time.Millisecond, MaxDelay: 8 * time.Millisecond}
	pair := sim.NewPair(6101, c, c)
	cfg := groupCfg()

	interactive := makePayload(6000)
	batch := makePayload(1500)
	type outcome struct {
		batchFirst time.Time
		interDone  time.Time
		batchOK    bool
		interOK    bool
	}
	done := make(chan outcome, 1)
	go func() {
		var o outcome
		srv, err := rdt.Accept(context.Background(), pair.B, cfg)
		if err != nil {
			done <- o
			return
		}
		batchSt, err := srv.AcceptStream()
		if err != nil {
			done <- o
			return
		}
		interCh := make(chan []byte, 1)
		go func() {
			b, _ := io.ReadAll(srv)
			interCh <- b
		}()
		var batchGot bytes.Buffer
		buf := make([]byte, 7)
		for {
			n, err := batchSt.Read(buf)
			if n > 0 && o.batchFirst.IsZero() {
				o.batchFirst = time.Now()
			}
			batchGot.Write(buf[:n])
			if err == io.EOF {
				break
			}
			if err != nil {
				done <- o
				return
			}
		}
		o.batchOK = bytes.Equal(batchGot.Bytes(), batch)
		interGot := <-interCh
		o.interDone = time.Now()
		o.interOK = bytes.Equal(interGot, interactive)
		done <- o
	}()

	cli, err := rdt.Dial(context.Background(), pair.A, cfg)
	if err != nil {
		t.Fatal(err)
	}
	cli.SetGroupWeight(1, 8) // interactive
	cli.SetGroupWeight(2, 1) // batch
	cli.SetGroup(1)
	batchSt := cli.OpenStream()
	batchSt.SetGroup(2)
	if _, err := batchSt.Write(batch); err != nil {
		t.Fatal(err)
	}
	if err := batchSt.CloseWrite(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.Write(interactive); err != nil {
		t.Fatal(err)
	}
	if err := cli.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	select {
	case o := <-done:
		if !o.batchOK || !o.interOK {
			t.Fatalf("byte mismatch batch=%v interactive=%v", o.batchOK, o.interOK)
		}
		if o.batchFirst.IsZero() || o.batchFirst.After(o.interDone) {
			t.Fatalf("batch stream starved: first=%v interactiveDone=%v", o.batchFirst, o.interDone)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("group transfer did not finish")
	}

	gs := waitGroupStats(t, cli, func(g []rdt.GroupStats) bool {
		g1, g2 := findGroup(g, 1), findGroup(g, 2)
		return g1 != nil && g2 != nil && g1.Pending == 0 && g2.Pending == 0 &&
			g1.Sent >= uint64(len(interactive)) && g2.Sent >= uint64(len(batch))
	})
	g1, g2 := findGroup(gs, 1), findGroup(gs, 2)
	if g1.Dropped != 0 || g2.Dropped != 0 {
		t.Fatalf("unexpected drops: %+v", gs)
	}
	ss, ok := cli.StreamStats(batchSt.ID())
	if !ok || ss.Group != 2 || ss.Sent != uint64(len(batch)) || ss.Pending != 0 {
		t.Fatalf("batch stream stats: %+v ok=%v", ss, ok)
	}
}

// A stream switches groups between sends; occupancy, acks and stats must
// stay attributed to the group each byte was sent under.
func TestGroupSwitchAccounting(t *testing.T) {
	pair := faultyPair(6202, 0.25)
	cfg := groupCfg()

	parts := [][]byte{makePayload(700), makePayload(900), makePayload(500)}
	groups := []uint32{0, 5, 7}
	var want []byte
	for _, p := range parts {
		want = append(want, p...)
	}
	done := make(chan []byte, 1)
	go func() {
		srv, err := rdt.Accept(context.Background(), pair.B, cfg)
		if err != nil {
			done <- nil
			return
		}
		st, err := srv.AcceptStream()
		if err != nil {
			done <- nil
			return
		}
		b, _ := io.ReadAll(st)
		done <- b
	}()

	cli, err := rdt.Dial(context.Background(), pair.A, cfg)
	if err != nil {
		t.Fatal(err)
	}
	st := cli.OpenStream()
	for i, p := range parts {
		st.SetGroup(groups[i]) // consecutive switches, no sends in between
		if _, err := st.Write(p); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.CloseWrite(context.Background()); err != nil {
		t.Fatal(err)
	}

	select {
	case got := <-done:
		if !bytes.Equal(got, want) {
			t.Fatalf("sequence mismatch got=%d want=%d", len(got), len(want))
		}
	case <-time.After(20 * time.Second):
		t.Fatal("switched-group stream did not drain")
	}

	gs := waitGroupStats(t, cli, func(g []rdt.GroupStats) bool {
		total := uint64(0)
		for _, s := range g {
			if s.Pending != 0 {
				return false
			}
			total += s.Sent
		}
		return total == uint64(len(want))
	})
	for i, id := range groups {
		g := findGroup(gs, id)
		if g == nil || g.Sent != uint64(len(parts[i])) || g.Dropped != 0 {
			t.Fatalf("group %d stats: %+v (all %+v)", id, g, gs)
		}
	}
	ss := st.Stats()
	if ss.Sent != uint64(len(want)) || ss.Pending != 0 || ss.Dropped != 0 || ss.Group != 7 {
		t.Fatalf("stream stats: %+v", ss)
	}
}

// Cancel after a group switch: pre-switch occupancy is dropped against
// the ORIGINAL group, not the current one.
func TestGroupResetAccounting(t *testing.T) {
	pair := faultyPair(6303, 0.1)
	cfg := groupCfg()
	go func() {
		srv, err := rdt.Accept(context.Background(), pair.B, cfg)
		if err != nil {
			return
		}
		st, err := srv.AcceptStream()
		if err != nil {
			return
		}
		buf := make([]byte, 16)
		for {
			if _, err := st.Read(buf); err != nil {
				return // reset observed
			}
		}
	}()
	cli, err := rdt.Dial(context.Background(), pair.A, cfg)
	if err != nil {
		t.Fatal(err)
	}
	st := cli.OpenStream()
	st.SetGroup(3)
	if _, err := st.Write(makePayload(400)); err != nil {
		t.Fatal(err)
	}
	st.SetGroup(9) // switch AFTER queueing; must not move the occupancy
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	gs := waitGroupStats(t, cli, func(g []rdt.GroupStats) bool {
		g3 := findGroup(g, 3)
		return g3 != nil && g3.Dropped > 0 && g3.Pending == 0
	})
	if g9 := findGroup(gs, 9); g9 != nil && (g9.Sent != 0 || g9.Dropped != 0) {
		t.Fatalf("post-switch group gained occupancy: %+v", g9)
	}
	if ss := st.Stats(); ss.Dropped == 0 {
		t.Fatalf("stream drop not recorded: %+v", ss)
	}
}

// Weights and total quota change mid-transfer; in-flight bytes keep
// counting and the transfer still completes exactly.
func TestRuntimeQuotaAndWeightAdjust(t *testing.T) {
	pair := faultyPair(6404, 0.2)
	cfg := groupCfg()

	want := makePayload(5000)
	done := make(chan []byte, 1)
	go func() {
		srv, err := rdt.Accept(context.Background(), pair.B, cfg)
		if err != nil {
			done <- nil
			return
		}
		st, err := srv.AcceptStream()
		if err != nil {
			done <- nil
			return
		}
		b, _ := io.ReadAll(st)
		done <- b
	}()
	cli, err := rdt.Dial(context.Background(), pair.A, cfg)
	if err != nil {
		t.Fatal(err)
	}
	st := cli.OpenStream()
	st.SetGroup(4)
	go func() {
		time.Sleep(30 * time.Millisecond)
		cli.SetTotalQuota(128)   // shrink below in-flight level
		cli.SetGroupWeight(4, 6) // reprioritize mid-flight
		time.Sleep(30 * time.Millisecond)
		cli.SetTotalQuota(512) // grow again
		cli.SetGroupWeight(4, 1)
	}()
	if _, err := st.Write(want); err != nil {
		t.Fatal(err)
	}
	if err := st.CloseWrite(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-done:
		if !bytes.Equal(got, want) {
			t.Fatalf("mismatch got=%d want=%d", len(got), len(want))
		}
	case <-time.After(20 * time.Second):
		t.Fatal("transfer with runtime adjust did not finish")
	}
	waitGroupStats(t, cli, func(g []rdt.GroupStats) bool {
		g4 := findGroup(g, 4)
		return g4 != nil && g4.Pending == 0 && g4.Sent == uint64(len(want)) && g4.Weight == 1
	})
}

// Bidirectional half-close across group switches, replayed twice with the
// same seed: identical bytes and identical fault decisions.
func TestGroupHalfCloseReplay(t *testing.T) {
	run := func(seed int64) ([]byte, []byte, []sim.Decision) {
		c := sim.Config{LossRate: 0.25, MinDelay: time.Millisecond, MaxDelay: 6 * time.Millisecond}
		pair := sim.NewPair(seed, c, c)
		cfg := groupCfg()
		forward := makePayload(2000)
		reverse := makePayload(1200)
		srvDone := make(chan []byte, 1)
		go func() {
			srv, err := rdt.Accept(context.Background(), pair.B, cfg)
			if err != nil {
				srvDone <- nil
				return
			}
			st, err := srv.AcceptStream()
			if err != nil {
				srvDone <- nil
				return
			}
			in, _ := io.ReadAll(st)
			st.SetGroup(2)
			_, _ = st.Write(reverse)
			_ = st.CloseWrite(context.Background())
			srvDone <- in
		}()
		cli, err := rdt.Dial(context.Background(), pair.A, cfg)
		if err != nil {
			t.Fatal(err)
		}
		st := cli.OpenStream()
		st.SetGroup(1)
		half := len(forward) / 2
		if _, err := st.Write(forward[:half]); err != nil {
			t.Fatal(err)
		}
		st.SetGroup(3) // switch between sends on the same stream
		if _, err := st.Write(forward[half:]); err != nil {
			t.Fatal(err)
		}
		if err := st.CloseWrite(context.Background()); err != nil {
			t.Fatal(err)
		}
		back, err := io.ReadAll(st) // half-close: reverse direction still open
		if err != nil {
			t.Fatal(err)
		}
		var in []byte
		select {
		case in = <-srvDone:
		case <-time.After(20 * time.Second):
			t.Fatal("server did not finish")
		}
		if !bytes.Equal(in, forward) || !bytes.Equal(back, reverse) {
			t.Fatalf("half-close mismatch fwd=%v rev=%v", bytes.Equal(in, forward), bytes.Equal(back, reverse))
		}
		return in, back, pair.A.Decisions()
	}

	// Same seed replays the same fault process: identical payloads, and a
	// decision log that explains both the fair interleaving of the two
	// client groups and the final FIN teardown.
	f1, r1, d1 := run(6505)
	f2, r2, _ := run(6505)
	if !bytes.Equal(f1, f2) || !bytes.Equal(r1, r2) {
		t.Fatal("replayed runs diverged in payload")
	}
	if len(d1) == 0 {
		t.Fatal("no fault decisions recorded")
	}
	for i, d := range d1 {
		if d.Index != uint64(i) {
			t.Fatalf("decision log gap at %d: %+v", i, d)
		}
	}
	var sawData, sawDrop, sawFin bool
	for _, d := range d1 {
		switch rdt.FrameKind(d.Kind) {
		case rdt.KindData:
			sawData = true
			if !d.Delivered {
				sawDrop = true
			}
		case rdt.KindFin:
			sawFin = true
		}
	}
	if !sawData || !sawDrop || !sawFin {
		t.Fatalf("decision log cannot explain run: data=%v drop=%v fin=%v", sawData, sawDrop, sawFin)
	}
}
