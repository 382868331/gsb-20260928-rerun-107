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
	if gs.Dropped != gs.DroppedInflight+gs.DroppedQueued {
		t.Fatalf("group 7 dropped split: %+v", gs)
	}
	if gs.Sent != gs.Acked+gs.DroppedInflight {
		t.Fatalf("group 7 conservation: %+v", gs)
	}
	st, _ := cli.StreamStats(canceled.ID())
	if st.Dropped == 0 {
		t.Fatalf("stream dropped nothing: %+v", st)
	}
	if st.Dropped != st.DroppedInflight+st.DroppedQueued {
		t.Fatalf("stream dropped split: %+v", st)
	}
	if st.Dropped != gs.Dropped {
		t.Fatalf("stream dropped %d != group dropped %d", st.Dropped, gs.Dropped)
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

func waitStreamAcked(t *testing.T, s *rdt.Session, id uint32, acked uint64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		st, ok := s.StreamStats(id)
		if ok && st.Acked >= acked {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	st, _ := s.StreamStats(id)
	t.Fatalf("stream %d acked did not reach %d: %+v", id, acked, st)
}

// TestGroupCancelAttributesQueuedDrops reproduces the reported ledger
// bug: 6 bytes written under the old group, 6 more after switching, the
// receive window caps the transfer at 4 acknowledged bytes, and the
// stream is then reset. The stream drops 8 bytes; the old group must
// answer for its remaining 2 queued bytes and the new group for its 6,
// all as never-sent (queued) drops, and a repeated close must not
// count them again.
func TestGroupCancelAttributesQueuedDrops(t *testing.T) {
	c := sim.Config{LossRate: 0, MinDelay: 20 * time.Millisecond, MaxDelay: 40 * time.Millisecond}
	pair := sim.NewPair(60222, c, c)
	cfg := groupCfg()
	cfg.MaxSegment = 4
	cfg.SendBuffer = 64
	cfg.RecvBuffer = 4 // peer window closes after the first segment
	cfg.GroupQuota = 4 // one segment in flight at a time

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
		// Never read: the advertised window stays 0 after 4 bytes.
		srvErr <- nil
	}()

	cli, err := rdt.Dial(context.Background(), pair.A, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Abort()
	st := cli.OpenStream()
	st.SetGroup(1)
	if _, err := st.Write(makePayload(6)); err != nil { // 4 in flight, 2 queued
		t.Fatal(err)
	}
	st.SetGroup(2)
	if _, err := st.Write(makePayload(6)); err != nil { // all 6 queued
		t.Fatal(err)
	}
	waitStreamAcked(t, cli, st.ID(), 4)
	if got := cli.GroupStats(1); got.Sent != 4 || got.Acked != 4 || got.Inflight != 0 {
		t.Fatalf("group 1 before cancel: %+v", got)
	}

	snap := func() (rdt.StreamStats, rdt.GroupStats, rdt.GroupStats) {
		ss, _ := cli.StreamStats(st.ID())
		return ss, cli.GroupStats(1), cli.GroupStats(2)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	ss, g1, g2 := snap()
	if ss.Sent != 4 || ss.Acked != 4 || ss.Dropped != 8 ||
		ss.DroppedInflight != 0 || ss.DroppedQueued != 8 {
		t.Fatalf("stream stats after cancel: %+v", ss)
	}
	if g1.Dropped != 2 || g1.DroppedQueued != 2 || g1.DroppedInflight != 0 ||
		g1.Sent != 4 || g1.Acked != 4 || g1.Inflight != 0 {
		t.Fatalf("old group stats after cancel: %+v", g1)
	}
	if g2.Dropped != 6 || g2.DroppedQueued != 6 || g2.DroppedInflight != 0 ||
		g2.Sent != 0 || g2.Acked != 0 {
		t.Fatalf("new group stats after cancel: %+v", g2)
	}
	if g1.Dropped+g2.Dropped != ss.Dropped {
		t.Fatalf("group drops %d+%d != stream drops %d", g1.Dropped, g2.Dropped, ss.Dropped)
	}

	// A repeated close must not record the drop a second time.
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	ss2, g1b, g2b := snap()
	if ss2 != ss || g1b != g1 || g2b != g2 {
		t.Fatalf("second close changed the ledger: %+v %+v %+v", ss2, g1b, g2b)
	}

	select {
	case err := <-srvErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("server did not accept the stream")
	}
}

// TestGroupCancelInflightAndQueuedSplit cancels mid-flight after several
// group switches with different fragment splits, before any ACK arrives:
// sent-but-unacked bytes must land in DroppedInflight and never-sent
// bytes in DroppedQueued, each under the group that owned the fragment,
// and the group ledgers must sum to the stream ledger.
func TestGroupCancelInflightAndQueuedSplit(t *testing.T) {
	c := sim.Config{LossRate: 0, MinDelay: 20 * time.Millisecond, MaxDelay: 40 * time.Millisecond}
	pair := sim.NewPair(60223, c, c)
	cfg := groupCfg()
	cfg.MaxSegment = 4
	cfg.SendBuffer = 64
	cfg.RecvBuffer = 64
	cfg.GroupQuota = 8 // shared by groups 0..3, weight 1 each

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
		srvErr <- nil
	}()

	cli, err := rdt.Dial(context.Background(), pair.A, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Abort()
	st := cli.OpenStream()

	// Group 1: 8 bytes, both segments fit the quota -> 8 in flight.
	st.SetGroup(1)
	if _, err := st.Write(makePayload(8)); err != nil {
		t.Fatal(err)
	}
	// Group 2: quota exhausted, guaranteed share 8/3 -> 2 in flight, 2 queued.
	st.SetGroup(2)
	if _, err := st.Write(makePayload(4)); err != nil {
		t.Fatal(err)
	}
	// Group 3: its bytes sit behind group 2's queued tail in the stream's
	// ordered queue, and group 2's guaranteed share (8/4 = 2) is already
	// in flight, so nothing more can send -> all 6 queued.
	st.SetGroup(3)
	if _, err := st.Write(makePayload(6)); err != nil {
		t.Fatal(err)
	}
	// Cancel before any ACK can arrive (link latency is at least 20ms).
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	wantInflight := map[uint32]uint64{1: 8, 2: 2, 3: 0}
	wantQueued := map[uint32]uint64{1: 0, 2: 2, 3: 6}
	var sumSent, sumDrop uint64
	for g := uint32(1); g <= 3; g++ {
		gs := cli.GroupStats(g)
		if gs.DroppedInflight != wantInflight[g] || gs.DroppedQueued != wantQueued[g] {
			t.Fatalf("group %d drops: %+v want inflight=%d queued=%d",
				g, gs, wantInflight[g], wantQueued[g])
		}
		if gs.Dropped != gs.DroppedInflight+gs.DroppedQueued {
			t.Fatalf("group %d dropped split: %+v", g, gs)
		}
		if gs.Sent != gs.Acked+gs.Inflight+gs.DroppedInflight {
			t.Fatalf("group %d ledger unbalanced: %+v", g, gs)
		}
		if gs.Inflight != 0 || gs.Acked != 0 {
			t.Fatalf("group %d settled bytes after cancel: %+v", g, gs)
		}
		sumSent += gs.Sent
		sumDrop += gs.Dropped
	}
	ss, ok := cli.StreamStats(st.ID())
	if !ok {
		t.Fatal("stream stats missing")
	}
	if ss.Sent != 10 || ss.DroppedInflight != 10 || ss.DroppedQueued != 8 || ss.Dropped != 18 {
		t.Fatalf("stream stats after cancel: %+v", ss)
	}
	if sumSent != ss.Sent || sumDrop != ss.Dropped {
		t.Fatalf("group sums sent=%d drop=%d != stream %+v", sumSent, sumDrop, ss)
	}
	if ss.Dropped != ss.DroppedInflight+ss.DroppedQueued {
		t.Fatalf("stream dropped split: %+v", ss)
	}

	// Repeated closes stay idempotent.
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	ss2, _ := cli.StreamStats(st.ID())
	if ss2 != ss {
		t.Fatalf("second close changed the ledger: %+v != %+v", ss2, ss)
	}

	select {
	case err := <-srvErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("server did not accept the stream")
	}
}

// TestGroupPeerResetKeepsAttribution resets from the peer side: the
// sender's unacked bytes are dropped under the group that sent them,
// ACKs and the RST may interleave, and a local close after the peer
// reset cannot move or re-count the charges.
func TestGroupPeerResetKeepsAttribution(t *testing.T) {
	c := sim.Config{LossRate: 0.1, MinDelay: time.Millisecond, MaxDelay: 8 * time.Millisecond}
	pair := sim.NewPair(60224, c, c)
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
		srvErr <- st.Close() // RST the stream as soon as it appears
	}()

	cli, err := rdt.Dial(context.Background(), pair.A, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Abort()
	st := cli.OpenStream()
	st.SetGroup(5)
	// Write blocks once the send buffer fills and unblocks with ErrClosed
	// when the peer reset arrives.
	go func() { _, _ = st.Write(makePayload(2000)) }()

	// Wait for the peer reset to land and the ledger to settle.
	deadline := time.Now().Add(10 * time.Second)
	for {
		gs := cli.GroupStats(5)
		if gs.Dropped > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	gs := cli.GroupStats(5)
	ss, _ := cli.StreamStats(st.ID())
	if gs.DroppedInflight == 0 {
		t.Fatalf("peer reset dropped no in-flight bytes: %+v", gs)
	}
	if gs.Sent != gs.Acked+gs.Inflight+gs.DroppedInflight || gs.Inflight != 0 {
		t.Fatalf("group 5 ledger unbalanced after peer reset: %+v", gs)
	}
	if gs.Dropped != gs.DroppedInflight+gs.DroppedQueued {
		t.Fatalf("group 5 dropped split: %+v", gs)
	}
	if ss.Dropped != gs.Dropped || ss.DroppedInflight != gs.DroppedInflight {
		t.Fatalf("stream %+v vs group %+v after peer reset", ss, gs)
	}

	// A local close after the peer reset is a no-op for the ledger.
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	gs2 := cli.GroupStats(5)
	ss2, _ := cli.StreamStats(st.ID())
	if gs2 != gs || ss2 != ss {
		t.Fatalf("close after peer reset changed the ledger: %+v %+v", gs2, ss2)
	}

	select {
	case err := <-srvErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("server did not reset the stream")
	}
}
