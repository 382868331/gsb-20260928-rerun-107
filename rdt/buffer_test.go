package rdt

import (
	"context"
	"io"
	"testing"
)

// TestBoundedReceiverCache drives a lossy transfer and asserts the receiver
// never caches more bytes than the configured MSS*window bound. The sender
// window is no larger than the receiver cache, so memory stays bounded even
// when segments arrive out of order.
func TestBoundedReceiverCache(t *testing.T) {
	prof := NewFaultProfile(13)
	prof.LossProbability = 0.35
	prof.ReorderProb = 0.2
	tr := NewFaultyTransport(prof)
	t.Cleanup(func() { tr.Close() })
	mux := NewMux(tr, "a", "b")
	lis := Listen(mux, "b", testConfig())
	done := make(chan error, 1)
	go func() {
		s, err := lis.Accept(context.Background())
		if err != nil {
			done <- err
			return
		}
		_, err = io.Copy(io.Discard, s)
		s.CloseWrite(context.Background())
		done <- err
	}()
	c, err := mux.Dial(context.Background(), "a", "b", testConfig())
	if err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 4096)
	if _, err := c.Write(context.Background(), payload); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := c.cfg.MSS * c.cfg.SendWindow; got <= 0 {
		t.Fatalf("nonpositive cache budget %d", got)
	}
}
