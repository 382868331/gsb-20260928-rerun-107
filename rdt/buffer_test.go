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

// slowConn delays Recv reads by holding the receiver open while a slow
// application reads, exercising both the send and receive buffer bounds.
func TestBoundedBuffersComplete(t *testing.T) {
	c := sim.Config{LossRate: 0.2, MinDelay: time.Millisecond, MaxDelay: 10 * time.Millisecond}
	pair := sim.NewPair(321, c, c)

	cfg := rdt.Config{
		MaxSegment:       128,
		SendBuffer:       512,
		RecvBuffer:       640,
		HandshakeTimeout: 50 * time.Millisecond,
		RTO:              50 * time.Millisecond,
	}

	got := make(chan []byte, 1)
	go func() {
		srv, err := rdt.Accept(context.Background(), pair.B, cfg)
		if err != nil {
			got <- nil
			return
		}
		// Read very slowly through a tiny application buffer so the receive
		// buffer and advertised window are repeatedly saturated.
		var buf bytes.Buffer
		tmp := make([]byte, 17)
		for {
			n, err := srv.Read(tmp)
			buf.Write(tmp[:n])
			time.Sleep(2 * time.Millisecond)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Errorf("read: %v", err)
				break
			}
		}
		srv.Close(context.Background())
		got <- buf.Bytes()
	}()

	cli, err := rdt.Dial(context.Background(), pair.A, cfg)
	if err != nil {
		t.Fatal(err)
	}
	payload := makePayload(8000)
	if _, err := cli.Write(payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := cli.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case b := <-got:
		if !bytes.Equal(b, payload) {
			t.Fatalf("mismatch got=%d want=%d", len(b), len(payload))
		}
	case <-time.After(30 * time.Second):
		t.Fatal("timeout despite bounded buffers")
	}
}
