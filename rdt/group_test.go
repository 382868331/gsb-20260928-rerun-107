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
	return rdt.Config{
		MaxSegment:       128,
		SendBuffer:       512,
		RecvBuffer:       640,
		GroupQuota:       1024,
		HandshakeTimeout: 50 * time.Millisecond,
		RTO:              50 * time.Millisecond,
	}
}

func waitStreamInflightZero(t *testing.T, s *rdt.Session, id uint32) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		st, ok := s.StreamStats(id)
		if ok && st.Inflight == 0 {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("stream %d inflight did not drain", id)
}

func TestGroupFairnessUnderBusyPriority(t *testing.T) {
	c := sim.Config{LossRate: 0.2, MinDelay: time.Millisecond, MaxDelay: 8 * time.Millisecond}
	pair := sim.NewPair(5150, c, c)
	cfg := groupCfg()

	interactive := makePayload(4000)
	batch := makePayload(4000)
	srvErr := make(chan error, 1)
	go func() {
		srv, err := rdt.Accept(context.Background(), pair.B, cfg)
		if err != nil {
			srvErr <- err
			return
		}
		s1, err1 := srv.AcceptStream()
		s2, err2 := srv.AcceptStream()
		if err1 != nil || err2 != nil {
			srvErr <- io.ErrUnexpectedEOF
			return
		}
		var g1, g2 bytes.Buffer
		done := make(chan struct{}, 2)
		go func() { _, _ = io.CopyBuffer(&g1, s1, make([]byte, 64)); done <- struct{}{} }()
		go func() { _, _ = io.CopyBuffer(&g2, s2, make([]byte, 64)); done <- struct{}{} }()
		<-done
		<-done
		if !bytes.Equal(g1.Bytes(), interactive) || !bytes.Equal(g2.Bytes(), batch) {
			srvErr <- io.ErrShortBuffer
			return
		}
		srvErr <- nil
	}()

	cli, err := rdt.Dial(context.Background(), pair.A, cfg)
	if err != nil {
		t.Fatal(err)
	}
	cli.SetGroupWeight(1, 4) // interactive priority group
	cli.SetGroupWeight(2, 1) // batch group
	s1 := cli.OpenStream()
	s2 := cli.OpenStream()
	s1.SetGroup(1)
	s2.SetGroup(2)

	w1 := make(chan error, 1)
	w2 := make(chan error, 1)
	go func() { _, err := s1.Write(interactive); w1 <- err }()
	go func() { _, err := s2.Write(batch); w2 <- err }()
	for i, ch := range []chan error{w1, w2} {
		select {
		case err := <-ch:
			if err != nil {
				t.Fatalf("write %d: %v", i, err)
			}
		case <-time.After(20 * time.Second):
			t.Fatalf("stream %d starved", i)
		}
	}
	if err := s1.CloseWrite(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s2.CloseWrite(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-srvErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("server did not drain both groups")
	}

	for _, g := range []uint32{1, 2} {
		gs := cli.GroupStats(g)
		if gs.Sent != uint64(len(interactive)) || gs.Acked != gs.Sent || gs.Inflight != 0 {
			t.Fatalf("group %d stats: %+v", g, gs)
		}
	}
	st1, _ := cli.StreamStats(s1.ID())
	st2, _ := cli.StreamStats(s2.ID())
	if st1.Sent != uint64(len(interactive)) || st2.Sent != uint64(len(batch)) {
		t.Fatalf("stream stats: %+v %+v", st1, st2)
	}
}

func TestRuntimeQuotaAndWeightAdjust(t *testing.T) {
	c := sim.Config{LossRate: 0.25, MinDelay: time.Millisecond, MaxDelay: 8 * time.Millisecond}
	pair := sim.NewPair(777, c, c)
	cfg := groupCfg()
	cfg.GroupQuota = 2048

	payload := makePayload(6000)
	srvErr := make(chan error, 1)
	go func() {
		srv, err := rdt.Accept(context.Background(), pair.B, cfg)
		if err != nil {
			srvErr <- err
			return
		}
		var got bytes.Buffer
		_, _ = io.CopyBuffer(&got, srv, make([]byte, 33))
		if !bytes.Equal(got.Bytes(), payload) {
			srvErr <- io.ErrShortBuffer
			return
		}
		srvErr <- nil
	}()

	cli, err := rdt.Dial(context.Background(), pair.A, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got := cli.TotalQuota(); got != 2048 {
		t.Fatalf("initial quota=%d", got)
	}
	done := make(chan error, 1)
	go func() { _, err := cli.Write(payload); done <- err }()

	// Shrink below the in-flight level, reweight, then grow again, all
	// while the transfer is running.
	time.Sleep(30 * time.Millisecond)
	cli.SetTotalQuota(300)
	cli.SetGroupWeight(0, 8)
	time.Sleep(30 * time.Millisecond)
	cli.SetTotalQuota(4096)
	cli.SetGroupWeight(0, 1)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("write: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("write stalled after runtime quota adjust")
	}
	if err := cli.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-srvErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("server did not finish")
	}
	if got := cli.TotalQuota(); got != 4096 {
		t.Fatalf("quota=%d", got)
	}
	cli.SetTotalQuota(1) // clamped to at least one segment
	if got := cli.TotalQuota(); got != uint64(cfg.MaxSegment) {
		t.Fatalf("clamped quota=%d", got)
	}
	gs := cli.GroupStats(0)
	if gs.Sent != uint64(len(payload)) || gs.Acked != gs.Sent || gs.Inflight != 0 {
		t.Fatalf("group stats after adjust: %+v", gs)
	}
}

func TestGroupSwitchAttributesCharges(t *testing.T) {
	c := sim.Config{LossRate: 0.15, MinDelay: time.Millisecond, MaxDelay: 6 * time.Millisecond}
	pair := sim.NewPair(9090, c, c)
	cfg := groupCfg()

	const rounds = 5
	const chunk = 300
	var full []byte
	for i := 0; i < rounds; i++ {
		full = append(full, makePayload(chunk)...)
	}
	srvErr := make(chan error, 1)
	go func() {
		srv, err := rdt.Accept(context.Background(), pair.B, cfg)
		if err != nil {
			srvErr <- err
			return
		}
		var got bytes.Buffer
		_, _ = io.CopyBuffer(&got, srv, make([]byte, 50))
		if !bytes.Equal(got.Bytes(), full) {
			srvErr <- io.ErrShortBuffer
			return
		}
		srvErr <- nil
	}()

	cli, err := rdt.Dial(context.Background(), pair.A, cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Consecutive group switches between sends; each chunk is charged to
	// the group active when it was transmitted.
	wantPerGroup := map[uint32]uint64{}
	for i := 0; i < rounds; i++ {
		g := uint32(i%3 + 1)
		if err := cli.SetStreamGroup(0, g); err != nil {
			t.Fatal(err)
		}
		if _, err := cli.Write(makePayload(chunk)); err != nil {
			t.Fatal(err)
		}
		waitStreamInflightZero(t, cli, 0)
		wantPerGroup[g] += chunk
	}
	if err := cli.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-srvErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("server did not finish")
	}

	var groupSentSum uint64
	for g := uint32(1); g <= 3; g++ {
		gs := cli.GroupStats(g)
		if gs.Acked != wantPerGroup[g] || gs.Sent != wantPerGroup[g] || gs.Inflight != 0 {
			t.Fatalf("group %d: got %+v want acked=%d", g, gs, wantPerGroup[g])
		}
		groupSentSum += gs.Sent
	}
	st, ok := cli.StreamStats(0)
	if !ok {
		t.Fatal("stream stats missing")
	}
	if st.Sent != uint64(len(full)) || st.Acked != st.Sent || groupSentSum != st.Sent {
		t.Fatalf("conservation: stream=%+v groupSum=%d", st, groupSentSum)
	}
	if st.Group != 2 { // last switch: (4%3)+1
		t.Fatalf("final group=%d", st.Group)
	}
}

func TestGroupSwitchPinsQueuedBytes(t *testing.T) {
	c := sim.Config{LossRate: 0.1, MinDelay: 20 * time.Millisecond, MaxDelay: 40 * time.Millisecond}
	pair := sim.NewPair(60221, c, c)
	cfg := groupCfg()
	cfg.MaxSegment = 4
	cfg.SendBuffer = 64
	cfg.RecvBuffer = 64
	cfg.GroupQuota = 4 // one segment in flight at a time

	oldGroup := makePayload(6)
	newGroup := makePayload(10)
	full := append(append([]byte{}, oldGroup...), newGroup...)
	srvErr := make(chan error, 1)
	go func() {
		srv, err := rdt.Accept(context.Background(), pair.B, cfg)
		if err != nil {
			srvErr <- err
			return
		}
		var got bytes.Buffer
		_, _ = io.CopyBuffer(&got, srv, make([]byte, 3))
		if !bytes.Equal(got.Bytes(), full) {
			srvErr <- io.ErrShortBuffer
			return
		}
		srvErr <- nil
	}()

	cli, err := rdt.Dial(context.Background(), pair.A, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.SetStreamGroup(0, 1); err != nil {
		t.Fatal(err)
	}
	// Quota 4 with MSS 4: the first segment goes in flight, the last 2
	// bytes stay queued. Switching groups now must not move them.
	if _, err := cli.Write(oldGroup); err != nil {
		t.Fatal(err)
	}
	if err := cli.SetStreamGroup(0, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.Write(newGroup); err != nil {
		t.Fatal(err)
	}
	// Shrink the budget below the in-flight level mid-transfer; existing
	// charges keep their group and only new sends pause.
	cli.SetTotalQuota(8)
	time.Sleep(10 * time.Millisecond)
	cli.SetTotalQuota(4)
	if err := cli.CloseWrite(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := cli.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-srvErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("server did not finish")
	}

	g1 := cli.GroupStats(1)
	if g1.Sent != uint64(len(oldGroup)) || g1.Acked != g1.Sent || g1.Inflight != 0 || g1.Dropped != 0 {
		t.Fatalf("group 1 (old) stats: %+v, want sent=acked=%d", g1, len(oldGroup))
	}
	g2 := cli.GroupStats(2)
	if g2.Sent != uint64(len(newGroup)) || g2.Acked != g2.Sent || g2.Inflight != 0 || g2.Dropped != 0 {
		t.Fatalf("group 2 (new) stats: %+v, want sent=acked=%d", g2, len(newGroup))
	}
	for id, g := range map[uint32]rdt.GroupStats{1: g1, 2: g2} {
		if g.Sent != g.Acked+g.Inflight+g.Dropped {
			t.Fatalf("group %d ledger unbalanced: %+v", id, g)
		}
	}
	st, ok := cli.StreamStats(0)
	if !ok {
		t.Fatal("stream stats missing")
	}
	if st.Sent != uint64(len(full)) || st.Acked != st.Sent || st.Inflight != 0 || st.Dropped != 0 {
		t.Fatalf("stream stats: %+v, want sent=acked=%d", st, len(full))
	}
	if g1.Sent+g2.Sent != st.Sent {
		t.Fatalf("group sum %d != stream sent %d", g1.Sent+g2.Sent, st.Sent)
	}
	if st.Group != 2 {
		t.Fatalf("final group=%d", st.Group)
	}
}

func TestGroupResetAccounting(t *testing.T) {
	c := sim.Config{LossRate: 0.2, MinDelay: time.Millisecond, MaxDelay: 8 * time.Millisecond}
	pair := sim.NewPair(31337, c, c)
	cfg := groupCfg()

	good := makePayload(3000)
	srvErr := make(chan error, 1)
	go func() {
		srv, err := rdt.Accept(context.Background(), pair.B, cfg)
		if err != nil {
			srvErr <- err
			return
		}
		canceled, err := srv.AcceptStream()
		if err != nil {
			srvErr <- err
			return
		}
		goodSt, err := srv.AcceptStream()
		if err != nil {
			srvErr <- err
			return
		}
		buf := make([]byte, 16)
		for {
			if _, err := canceled.Read(buf); err == rdt.ErrClosed {
				break
			} else if err != nil {
				srvErr <- err
				return
			}
		}
		got, err := io.ReadAll(goodSt)
		if err != nil || !bytes.Equal(got, good) {
			srvErr <- io.ErrShortBuffer
			return
		}
		srvErr <- nil
	}()

	cli, err := rdt.Dial(context.Background(), pair.A, cfg)
	if err != nil {
		t.Fatal(err)
	}
	canceled := cli.OpenStream()
	canceled.SetGroup(7)
	goodSt := cli.OpenStream()
	go canceled.Write(makePayload(2000)) // blocks, then unblocks on reset
	time.Sleep(50 * time.Millisecond)
	canceled.Close() // reset: in-flight and queued bytes become dropped

	if _, err := goodSt.Write(good); err != nil {
		t.Fatal(err)
	}
	if err := goodSt.CloseWrite(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-srvErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("good stream blocked by reset stream")
	}

	gs := cli.GroupStats(7)
	if gs.Dropped == 0 {
		t.Fatalf("reset group dropped nothing: %+v", gs)
	}
	if gs.Sent != gs.Acked+gs.Dropped {
		t.Fatalf("group 7 conservation: %+v", gs)
	}
	st, _ := cli.StreamStats(canceled.ID())
	if st.Dropped == 0 {
		t.Fatalf("stream dropped nothing: %+v", st)
	}
}

// runGroupHalfClose runs one bidirectional half-close session with groups
// and returns both directions' bytes plus the sender-side decisions.
func runGroupHalfClose(t *testing.T, seed int64) (fwd, rev []byte, decAB, decBA []sim.Decision) {
	t.Helper()
	c := sim.Config{LossRate: 0.2, MinDelay: time.Millisecond, MaxDelay: 8 * time.Millisecond}
	pair := sim.NewPair(seed, c, c)
	cfg := groupCfg()

	forward := makePayload(2500)
	reverse := makePayload(1800)
	srvOut := make(chan []byte, 1)
	go func() {
		srv, err := rdt.Accept(context.Background(), pair.B, cfg)
		if err != nil {
			srvOut <- nil
			return
		}
		st, err := srv.AcceptStream()
		if err != nil {
			srvOut <- nil
			return
		}
		var got bytes.Buffer
		done := make(chan struct{})
		go func() { _, _ = io.CopyBuffer(&got, st, make([]byte, 64)); close(done) }()
		if _, err := st.Write(reverse); err != nil {
			srvOut <- nil
			return
		}
		_ = st.CloseWrite(context.Background())
		<-done
		srvOut <- got.Bytes()
	}()

	cli, err := rdt.Dial(context.Background(), pair.A, cfg)
	if err != nil {
		t.Fatal(err)
	}
	cli.SetGroupWeight(1, 3)
	st := cli.OpenStream()
	st.SetGroup(1)
	if _, err := st.Write(forward); err != nil {
		t.Fatal(err)
	}
	if err := st.CloseWrite(context.Background()); err != nil {
		t.Fatal(err)
	}
	revGot, err := io.ReadAll(st)
	if err != nil {
		t.Fatalf("read reverse: %v", err)
	}
	select {
	case got := <-srvOut:
		if got == nil {
			t.Fatal("server failed")
		}
		fwd = got
	case <-time.After(20 * time.Second):
		t.Fatal("server did not finish")
	}
	return fwd, revGot, pair.A.Decisions(), pair.B.Decisions()
}

func TestGroupHalfCloseReplayable(t *testing.T) {
	fwd1, rev1, decAB, decBA := runGroupHalfClose(t, 4242)
	fwd2, rev2, _, _ := runGroupHalfClose(t, 4242)
	if !bytes.Equal(fwd1, makePayload(2500)) || !bytes.Equal(rev1, makePayload(1800)) {
		t.Fatal("byte sequences mismatch")
	}
	if !bytes.Equal(fwd1, fwd2) || !bytes.Equal(rev1, rev2) {
		t.Fatal("same seed replayed to different bytes")
	}
	// The saved fault decisions must explain the run: data frames for the
	// grouped stream, observable losses (fair waiting), and FIN/FINACK for
	// the final wrap-up.
	var data, lost, fin, finAck bool
	for _, d := range decAB {
		switch uint8(d.Kind) {
		case uint8(rdt.KindData):
			data = true
			if !d.Delivered {
				lost = true
			}
		case uint8(rdt.KindFin):
			fin = true
		case uint8(rdt.KindFinAck):
		}
	}
	for _, d := range decBA {
		if uint8(d.Kind) == uint8(rdt.KindFinAck) {
			finAck = true
		}
	}
	if !data || !lost || !fin || !finAck {
		t.Fatalf("decisions incomplete: data=%v lost=%v fin=%v finack=%v", data, lost, fin, finAck)
	}
}

// waitStreamStats polls the stream stats until cond holds or the deadline
// passes, failing the test on timeout.
func waitStreamStats(t *testing.T, s *rdt.Session, id uint32, what string, cond func(rdt.StreamStats) bool) rdt.StreamStats {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		st, ok := s.StreamStats(id)
		if ok && cond(st) {
			return st
		}
		time.Sleep(2 * time.Millisecond)
	}
	st, _ := s.StreamStats(id)
	t.Fatalf("stream %d stats never satisfied %s: %+v", id, what, st)
	return rdt.StreamStats{}
}

// cancelCfg returns a config whose receive window admits exactly one
// 4-byte segment and then stays shut while the server never reads.
func cancelCfg() rdt.Config {
	cfg := groupCfg()
	cfg.MaxSegment = 4
	cfg.SendBuffer = 64
	cfg.RecvBuffer = 4
	return cfg
}

// TestGroupCancelAttributesQueuedDrops is the regression test for queued
// (never sent) bytes missing from the per-group drop ledger: 6 bytes are
// written in the old group, then 6 more after switching to a new group,
// while the receive window lets only 4 bytes be acknowledged. Cancelling
// the stream must drop 8 bytes at stream level, split 2 (old group
// remainder) and 6 (new group) at group level, without confusing
// sent-but-unacked drops with never-sent ones.
func TestGroupCancelAttributesQueuedDrops(t *testing.T) {
	c := sim.Config{LossRate: 0, MinDelay: time.Millisecond, MaxDelay: 3 * time.Millisecond}
	pair := sim.NewPair(777001, c, c)
	cfg := cancelCfg()

	srvErr := make(chan error, 1)
	go func() {
		srv, err := rdt.Accept(context.Background(), pair.B, cfg)
		if err != nil {
			srvErr <- err
			return
		}
		if _, err := srv.AcceptStream(); err != nil {
			srvErr <- err
			return
		}
		// Never read: the receive window stays shut after 4 bytes.
		time.Sleep(2 * time.Second)
		srvErr <- nil
	}()

	cli, err := rdt.Dial(context.Background(), pair.A, cfg)
	if err != nil {
		t.Fatal(err)
	}
	st := cli.OpenStream()
	st.SetGroup(1)
	if _, err := st.Write(makePayload(6)); err != nil {
		t.Fatal(err)
	}
	st.SetGroup(2)
	if _, err := st.Write(makePayload(6)); err != nil {
		t.Fatal(err)
	}

	// Only the first 4 bytes fit the receive window and get acked; the
	// old group's remaining 2 bytes and all 6 of the new group sit queued.
	waitStreamStats(t, cli, st.ID(), "acked=4 inflight=0", func(ss rdt.StreamStats) bool {
		return ss.Acked == 4 && ss.Inflight == 0
	})

	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	g1 := cli.GroupStats(1)
	if g1.Sent != 4 || g1.Acked != 4 || g1.Inflight != 0 || g1.Dropped != 2 {
		t.Fatalf("group 1 (old) stats: %+v, want sent=4 acked=4 dropped=2", g1)
	}
	g2 := cli.GroupStats(2)
	if g2.Sent != 0 || g2.Acked != 0 || g2.Inflight != 0 || g2.Dropped != 6 {
		t.Fatalf("group 2 (new) stats: %+v, want sent=0 dropped=6", g2)
	}
	ss, ok := cli.StreamStats(st.ID())
	if !ok {
		t.Fatal("stream stats missing")
	}
	if ss.Sent != 4 || ss.Acked != 4 || ss.Dropped != 8 {
		t.Fatalf("stream stats: %+v, want sent=4 acked=4 dropped=8", ss)
	}
	if g1.Dropped+g2.Dropped != ss.Dropped {
		t.Fatalf("group drops %d+%d != stream drops %d", g1.Dropped, g2.Dropped, ss.Dropped)
	}
	// Repeating the close must not record the drop a second time.
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * cfg.RTO)
	if g := cli.GroupStats(1); g != g1 {
		t.Fatalf("group 1 changed after repeated close: %+v -> %+v", g1, g)
	}
	if g := cli.GroupStats(2); g != g2 {
		t.Fatalf("group 2 changed after repeated close: %+v -> %+v", g2, g)
	}
	if ss2, _ := cli.StreamStats(st.ID()); ss2.Dropped != ss.Dropped {
		t.Fatalf("stream dropped changed after repeated close: %d -> %d", ss.Dropped, ss2.Dropped)
	}
	select {
	case err := <-srvErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("server did not finish")
	}
}

// TestGroupCancelInterleavedAcksBalance cancels while acknowledgements
// are still arriving under loss, so the drop mixes sent-but-unacked
// segments with never-sent queued chunks. Exact counts are racy, but the
// ledger must stay balanced: every written byte ends up acked or dropped,
// in every group and in the stream totals.
func TestGroupCancelInterleavedAcksBalance(t *testing.T) {
	c := sim.Config{LossRate: 0.2, MinDelay: time.Millisecond, MaxDelay: 6 * time.Millisecond}
	pair := sim.NewPair(777003, c, c)
	cfg := groupCfg()

	srvErr := make(chan error, 1)
	go func() {
		srv, err := rdt.Accept(context.Background(), pair.B, cfg)
		if err != nil {
			srvErr <- err
			return
		}
		st, err := srv.AcceptStream()
		if err != nil {
			srvErr <- err
			return
		}
		buf := make([]byte, 64)
		for {
			if _, err := st.Read(buf); err != nil {
				srvErr <- nil
				return
			}
		}
	}()

	cli, err := rdt.Dial(context.Background(), pair.A, cfg)
	if err != nil {
		t.Fatal(err)
	}
	st := cli.OpenStream()
	written := map[uint32]uint64{1: 100, 2: 150, 3: 120}
	for _, step := range []struct{ g uint32; n int }{{1, 100}, {2, 150}, {3, 120}} {
		st.SetGroup(step.g)
		if _, err := st.Write(makePayload(step.n)); err != nil {
			t.Fatal(err)
		}
	}
	// Cancel immediately: in-flight segments race with their acks, and
	// the rest is still queued.
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	var total uint64
	var sumSent, sumAcked, sumDropped uint64
	for id, w := range written {
		total += w
		g := cli.GroupStats(id)
		if g.Acked+g.Dropped != w || g.Inflight != 0 {
			t.Fatalf("group %d ledger unbalanced: %+v, written=%d", id, g, w)
		}
		if g.Sent < g.Acked || g.Dropped < g.Sent-g.Acked {
			t.Fatalf("group %d sent/acked/dropped inconsistent: %+v", id, g)
		}
		sumSent += g.Sent
		sumAcked += g.Acked
		sumDropped += g.Dropped
	}
	ss, ok := cli.StreamStats(st.ID())
	if !ok {
		t.Fatal("stream stats missing")
	}
	if ss.Sent != sumSent || ss.Acked != sumAcked || ss.Dropped != sumDropped {
		t.Fatalf("stream ledger %+v vs group sums sent=%d acked=%d dropped=%d",
			ss, sumSent, sumAcked, sumDropped)
	}
	if ss.Acked+ss.Dropped != total || ss.Dropped == 0 {
		t.Fatalf("stream ledger unbalanced: %+v, written=%d", ss, total)
	}
	// A repeated close must not record anything again.
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * cfg.RTO)
	for id, w := range written {
		if g := cli.GroupStats(id); g.Acked+g.Dropped != w || g.Inflight != 0 {
			t.Fatalf("group %d ledger changed after repeated close: %+v", id, g)
		}
	}
	if ss2, _ := cli.StreamStats(st.ID()); ss2.Dropped != ss.Dropped {
		t.Fatalf("stream dropped changed after repeated close: %d -> %d", ss.Dropped, ss2.Dropped)
	}
	select {
	case err := <-srvErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("server did not finish")
	}
}

// TestGroupCancelLedgerMultiSwitch exercises several group switches with
// different chunk splits while acknowledgements arrive between them, then
// cancels: every group's ledger must balance (sent = acked + dropped) and
// the group totals must equal the stream totals.
func TestGroupCancelLedgerMultiSwitch(t *testing.T) {
	c := sim.Config{LossRate: 0, MinDelay: time.Millisecond, MaxDelay: 3 * time.Millisecond}
	pair := sim.NewPair(777002, c, c)
	cfg := cancelCfg()

	srvErr := make(chan error, 1)
	go func() {
		srv, err := rdt.Accept(context.Background(), pair.B, cfg)
		if err != nil {
			srvErr <- err
			return
		}
		if _, err := srv.AcceptStream(); err != nil {
			srvErr <- err
			return
		}
		// Never read: the window shuts after the first segment and every
		// ack carries window 0, so no late or reordered ack can reopen it.
		time.Sleep(2 * time.Second)
		srvErr <- nil
	}()

	cli, err := rdt.Dial(context.Background(), pair.A, cfg)
	if err != nil {
		t.Fatal(err)
	}
	st := cli.OpenStream()
	// The window admits exactly 4 bytes in total, and sends consume the
	// queue in write order, so the split is deterministic no matter how
	// the writes interleave with the acknowledgements: group 1 sends its
	// 3-byte chunk, group 2 gets 1 byte of its 5-byte chunk out, and the
	// rest stays queued until the cancel.
	st.SetGroup(1)
	if _, err := st.Write(makePayload(3)); err != nil {
		t.Fatal(err)
	}
	st.SetGroup(2)
	if _, err := st.Write(makePayload(5)); err != nil {
		t.Fatal(err)
	}
	st.SetGroup(3)
	if _, err := st.Write(makePayload(7)); err != nil {
		t.Fatal(err)
	}
	st.SetGroup(1)
	if _, err := st.Write(makePayload(2)); err != nil {
		t.Fatal(err)
	}
	st.SetGroup(2)
	if _, err := st.Write(makePayload(6)); err != nil {
		t.Fatal(err)
	}

	// The 4 window bytes are acknowledged before the cancel.
	waitStreamStats(t, cli, st.ID(), "acked=4 inflight=0", func(ss rdt.StreamStats) bool {
		return ss.Acked == 4 && ss.Inflight == 0
	})
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	want := map[uint32]rdt.GroupStats{
		1: {Sent: 3, Acked: 3, Dropped: 2},
		2: {Sent: 1, Acked: 1, Dropped: 10},
		3: {Sent: 0, Acked: 0, Dropped: 7},
	}
	written := map[uint32]uint64{1: 5, 2: 11, 3: 7}
	var sumSent, sumAcked, sumDropped uint64
	for id, w := range want {
		g := cli.GroupStats(id)
		if g.Sent != w.Sent || g.Acked != w.Acked || g.Inflight != 0 || g.Dropped != w.Dropped {
			t.Fatalf("group %d: got %+v want %+v", id, g, w)
		}
		// Every byte written in the group ends up acked or dropped, and
		// the sent-but-unacked part of the drop never exceeds sent-acked.
		if g.Acked+g.Dropped != written[id] || g.Sent < g.Acked || g.Dropped < g.Sent-g.Acked {
			t.Fatalf("group %d ledger unbalanced: %+v", id, g)
		}
		sumSent += g.Sent
		sumAcked += g.Acked
		sumDropped += g.Dropped
	}
	ss, ok := cli.StreamStats(st.ID())
	if !ok {
		t.Fatal("stream stats missing")
	}
	if ss.Sent != sumSent || ss.Acked != sumAcked || ss.Dropped != sumDropped || ss.Inflight != 0 {
		t.Fatalf("stream ledger %+v vs group sums sent=%d acked=%d dropped=%d",
			ss, sumSent, sumAcked, sumDropped)
	}
	// A repeated close must not record anything again.
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * cfg.RTO)
	for id := range want {
		g := cli.GroupStats(id)
		if g.Acked+g.Dropped != written[id] || g.Inflight != 0 {
			t.Fatalf("group %d ledger changed after repeated close: %+v", id, g)
		}
	}
	if ss2, _ := cli.StreamStats(st.ID()); ss2.Dropped != ss.Dropped {
		t.Fatalf("stream dropped changed after repeated close: %d -> %d", ss.Dropped, ss2.Dropped)
	}
	select {
	case err := <-srvErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("server did not finish")
	}
}
