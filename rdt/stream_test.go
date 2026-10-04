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

func drainStream(t *testing.T, st *rdt.Stream, want []byte) {
	t.Helper()
	var got bytes.Buffer
	buf := make([]byte, 128)
	for {
		n, err := st.Read(buf)
		got.Write(buf[:n])
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Errorf("stream %d: %v", st.ID(), err)
			return
		}
	}
	if !bytes.Equal(got.Bytes(), want) {
		t.Errorf("stream %d mismatch got=%d want=%d", st.ID(), len(got.Bytes()), len(want))
	}
}

func TestIndependentStreamsFairAndBounded(t *testing.T) {
	c := sim.Config{LossRate: 0.2, MinDelay: time.Millisecond, MaxDelay: 8 * time.Millisecond}
	pair := sim.NewPair(9182, c, c)
	cfg := fastCfg()
	cfg.MaxSegment = 64
	cfg.SendBuffer = 256
	cfg.RecvBuffer = 320

	fast := makePayload(6000)
	slow := makePayload(1200)
	srvDone := make(chan error, 1)
	go func() {
		srv, err := rdt.Accept(context.Background(), pair.B, cfg)
		if err != nil {
			srvDone <- err
			return
		}
		s2, err := srv.AcceptStream()
		if err != nil {
			srvDone <- err
			return
		}
		buf := make([]byte, 5)
		var got bytes.Buffer
		for {
			n, err := srv.Read(buf)
			got.Write(buf[:n])
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Errorf("fast stream: %v", err)
				return
			}
		}
		if !bytes.Equal(got.Bytes(), fast) {
			srvDone <- io.ErrShortBuffer
			return
		}
		drainStream(t, s2, slow)
		srvDone <- nil
	}()

	cli, err := rdt.Dial(context.Background(), pair.A, cfg)
	if err != nil {
		t.Fatal(err)
	}
	s2 := cli.OpenStream()
	w1 := make(chan error, 1)
	w2 := make(chan error, 1)
	go func() { _, err := cli.Write(fast); w1 <- err }()
	go func() { _, err := s2.Write(slow); w2 <- err }()
	select {
	case err := <-w1:
		if err != nil {
			t.Fatalf("fast write: %v", err)
		}
	case <-time.After(12 * time.Second):
		t.Fatal("fast stream was blocked by slow stream/window")
	}
	if err := cli.Close(context.Background()); err != nil {
		t.Fatalf("fast close write: %v", err)
	}
	select {
	case err := <-w2:
		if err != nil {
			t.Fatalf("slow write: %v", err)
		}
	case <-time.After(12 * time.Second):
		t.Fatal("slow stream did not complete after fast stream")
	}
	if err := s2.CloseWrite(context.Background()); err != nil {
		t.Fatalf("slow close write: %v", err)
	}
	select {
	case err := <-srvDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(12 * time.Second):
		t.Fatal("slow stream did not drain")
	}
}

func TestStreamResetDoesNotBlockOtherStream(t *testing.T) {
	pair := faultyPair(817, 0.25)
	cfg := fastCfg()
	result := make(chan error, 1)
	go func() {
		srv, err := rdt.Accept(context.Background(), pair.B, cfg)
		if err != nil {
			result <- err
			return
		}
		// Loss and reordering decide which stream is accepted first, so
		// identify them by behavior: one is reset, the other delivers.
		s1, err := srv.AcceptStream()
		if err != nil {
			result <- err
			return
		}
		s2, err := srv.AcceptStream()
		if err != nil {
			result <- err
			return
		}
		type readRes struct {
			data []byte
			err  error
		}
		readOne := func(st *rdt.Stream) readRes {
			var buf bytes.Buffer
			tmp := make([]byte, 64)
			for {
				n, err := st.Read(tmp)
				buf.Write(tmp[:n])
				if err != nil {
					return readRes{buf.Bytes(), err}
				}
			}
		}
		r1ch := make(chan readRes, 1)
		go func() { r1ch <- readOne(s1) }()
		r2 := readOne(s2)
		r1 := <-r1ch
		reset, delivered := r1, r2
		if r2.err == rdt.ErrClosed {
			reset, delivered = r2, r1
		}
		if reset.err != rdt.ErrClosed {
			result <- io.ErrUnexpectedEOF
			return
		}
		if delivered.err != io.EOF {
			result <- delivered.err
			return
		}
		result <- bytesCompare(delivered.data, makePayload(5000))
	}()
	cli, err := rdt.Dial(context.Background(), pair.A, cfg)
	if err != nil {
		t.Fatal(err)
	}
	canceled := cli.OpenStream()
	good := cli.OpenStream()
	if _, err := canceled.Write(makePayload(1000)); err != nil {
		t.Fatal(err)
	}
	canceled.Close()
	want := makePayload(5000)
	if _, err := good.Write(want); err != nil {
		t.Fatalf("good stream write: %v", err)
	}
	if err := good.CloseWrite(context.Background()); err != nil {
		t.Fatalf("good stream close: %v", err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("streams after reset: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("good stream blocked after reset")
	}
}

func bytesCompare(got, want []byte) error {
	if !bytes.Equal(got, want) {
		return io.ErrShortBuffer
	}
	return nil
}

func TestHalfCloseKeepsReceiving(t *testing.T) {
	pair := faultyPair(44, 0.15)
	srvDone := make(chan []byte, 1)
	go func() {
		srv, err := rdt.Accept(context.Background(), pair.B, fastCfg())
		if err != nil {
			return
		}
		in, _ := io.ReadAll(srv)
		_, _ = srv.Write([]byte("REVERSE-AFTER-HALF-CLOSE"))
		_ = srv.CloseWrite(context.Background())
		srvDone <- in
	}()
	cli, err := rdt.Dial(context.Background(), pair.A, fastCfg())
	if err != nil {
		t.Fatal(err)
	}
	want := []byte("FORWARD-BEFORE-HALF-CLOSE")
	if _, err := cli.Write(want); err != nil {
		t.Fatal(err)
	}
	if err := cli.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := cli.WaitForPeerClose(context.Background()); err != nil {
		t.Fatalf("wait peer close: %v", err)
	}
	reverse, err := io.ReadAll(cli)
	if err != nil {
		t.Fatalf("read after half-close: %v", err)
	}
	if string(reverse) != "REVERSE-AFTER-HALF-CLOSE" {
		t.Fatalf("reverse=%q", reverse)
	}
	select {
	case got := <-srvDone:
		if string(got) != string(want) {
			t.Fatalf("forward=%q", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not finish")
	}
}
