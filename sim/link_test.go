package sim_test

import (
	"context"
	"testing"
	"time"

	"rdt/rdt"
	"rdt/sim"
)

func TestDeterministicDuplicatesRecorded(t *testing.T) {
	c := sim.Config{DuplicateRate: 0.3, MinDelay: time.Millisecond, MaxDelay: 3 * time.Millisecond}
	pair := sim.NewPair(3301, c, c)
	cfg := rdt.Config{HandshakeTimeout: 50 * time.Millisecond, RTO: 50 * time.Millisecond}
	done := make(chan []byte, 1)
	go func() {
		srv, err := rdt.Accept(context.Background(), pair.B, cfg)
		if err != nil {
			return
		}
		buf := make([]byte, 64)
		n, _ := srv.Read(buf)
		srv.Close(context.Background())
		done <- buf[:n]
	}()
	cli, err := rdt.Dial(context.Background(), pair.A, cfg)
	if err != nil {
		t.Fatal(err)
	}
	want := []byte("duplicate-decision-replay")
	if _, err := cli.Write(want); err != nil {
		t.Fatal(err)
	}
	if err := cli.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-done:
		if string(got) != string(want) {
			t.Fatalf("got %q want %q", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout under duplicates")
	}
	decisions := pair.A.Decisions()
	if len(decisions) == 0 {
		t.Fatal("no fault decisions recorded")
	}
	first := append([]sim.Decision(nil), decisions...)
	second := pair.A.Decisions()
	if len(second) < len(first) {
		t.Fatal("decision history was lost")
	}
}
