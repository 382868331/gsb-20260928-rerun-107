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

// TestGroupSwitchPinsQueuedBytes reproduces the misattribution where
// bytes written under an old group but still queued (small segments,
// tight quota) were charged to the new group after a switch. The group
// of every fragment is pinned at Write time and must survive splitting,
// retransmission, acknowledgement and cancellation; the per-group and
// per-stream sent/inflight/dropped ledgers must reconcile after a
// runtime quota shrink and a half-close.
func TestGroupSwitchPinsQueuedBytes(t *testing.T) {
	c := sim.Config{LossRate: 0.2, MinDelay: 20 * time.Millisecond, MaxDelay: 40 * time.Millisecond}
	pair := sim.NewPair(60221, c, c)
	cfg := rdt.Config{
		MaxSegment:       2, // tiny segments force splitting
		SendBuffer:       32,
		RecvBuffer:       32,
		GroupQuota:       4, // tight: 6-byte write leaves 2 bytes queued
		HandshakeTimeout: 50 * time.Millisecond,
		RTO:              60 * time.Millisecond,
	}

	oldGroup := makePayload(6)
	newGroup := makePayload(10)
	canceled := makePayload(40)
	srvErr := make(chan error, 1)
	go func() {
		srv, err := rdt.Accept(context.Background(), pair.B, cfg)
		if err != nil {
			srvErr <- err
			return
		}
		resetSt, err := srv.AcceptStream()
		if err != nil {
			srvErr <- err
			return
		}
		readDone := make(chan error, 1)
		go func() {
			buf := make([]byte, 8)
			for {
				if _, err := resetSt.Read(buf); err == rdt.ErrClosed {
					readDone <- nil
					return
				} else if err != nil {
					readDone <- err
					return
				}
			}
		}()
		var got bytes.Buffer
		_, _ = io.CopyBuffer(&got, srv, make([]byte, 5))
		want := append(append([]byte{}, oldGroup...), newGroup...)
		if !bytes.Equal(got.Bytes(), want) {
			srvErr <- io.ErrShortBuffer
			return
		}
		if err := <-readDone; err != nil {
			srvErr <- err
			return
		}
		srvErr <- nil
	}()

	cli, err := rdt.Dial(context.Background(), pair.A, cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Six bytes under the default group 0: 4 go in flight (quota), 2 stay
	// queued. The one-way delay exceeds the switch latency, so the queued
	// bytes are still unsent when the stream changes group.
	if _, err := cli.Write(oldGroup); err != nil {
		t.Fatal(err)
	}
	if err := cli.SetStreamGroup(0, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.Write(newGroup); err != nil {
		t.Fatal(err)
	}
	// Shrink the quota below the in-flight level: occupancy stays charged
	// to the pinned groups and only new sends pause.
	cli.SetTotalQuota(2)
	time.Sleep(80 * time.Millisecond)
	g0 := cli.GroupStats(0)
	if g0.Sent != g0.Acked+g0.Inflight+g0.Dropped {
		t.Fatalf("group 0 ledger unbalanced after shrink: %+v", g0)
	}
	if g0.Sent > uint64(len(oldGroup)) {
		t.Fatalf("group 0 overcharged: %+v", g0)
	}
	cli.SetTotalQuota(64)

	// A second stream canceled after the switch: its dropped bytes must
	// land on its own group without disturbing groups 0 and 1.
	resetSt := cli.OpenStream()
	resetSt.SetGroup(2)
	resetDone := make(chan error, 1)
	go func() { _, err := resetSt.Write(canceled); resetDone <- err }()
	time.Sleep(50 * time.Millisecond)
	if err := resetSt.Close(); err != nil {
		t.Fatal(err)
	}
	<-resetDone

	if err := cli.CloseWrite(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitStreamInflightZero(t, cli, 0)
	select {
	case err := <-srvErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("server did not finish")
	}

	// The 6 old-group bytes (including the 2 queued across the switch)
	// belong to group 0; only the post-switch write belongs to group 1.
	g0 = cli.GroupStats(0)
	if g0.Sent != uint64(len(oldGroup)) || g0.Acked != g0.Sent || g0.Inflight != 0 || g0.Dropped != 0 {
		t.Fatalf("group 0 final: %+v want sent=acked=%d", g0, len(oldGroup))
	}
	g1 := cli.GroupStats(1)
	if g1.Sent != uint64(len(newGroup)) || g1.Acked != g1.Sent || g1.Inflight != 0 || g1.Dropped != 0 {
		t.Fatalf("group 1 final: %+v want sent=acked=%d", g1, len(newGroup))
	}
	g2 := cli.GroupStats(2)
	if g2.Dropped == 0 || g2.Inflight != 0 || g2.Sent != g2.Acked+g2.Dropped {
		t.Fatalf("group 2 final: %+v", g2)
	}
	st, ok := cli.StreamStats(0)
	if !ok {
		t.Fatal("stream stats missing")
	}
	if st.Sent != uint64(len(oldGroup)+len(newGroup)) || st.Acked != st.Sent || st.Inflight != 0 {
		t.Fatalf("stream 0 final: %+v", st)
	}
	if g0.Sent+g1.Sent != st.Sent {
		t.Fatalf("group/stream sent mismatch: %d+%d != %d", g0.Sent, g1.Sent, st.Sent)
	}
	rst, _ := cli.StreamStats(resetSt.ID())
	if rst.Dropped == 0 || rst.Dropped != g2.Dropped {
		t.Fatalf("reset stream dropped=%d group dropped=%d", rst.Dropped, g2.Dropped)
	}
	if err := cli.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
