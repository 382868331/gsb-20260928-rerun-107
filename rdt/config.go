package rdt

import "time"

// Config tunes a Session. Zero values select safe defaults.
type Config struct {
	// MaxSegment bounds one Data frame payload in bytes.
	MaxSegment int
	// SendBuffer bounds sender bytes that are queued or unacknowledged.
	SendBuffer int
	// RecvBuffer bounds receiver bytes held (ordered + out of order).
	RecvBuffer int
	// HandshakeTimeout bounds a single handshake step retransmission wait.
	HandshakeTimeout time.Duration
	// RTO is the retransmission timeout for data and FIN segments.
	RTO time.Duration
	// CloseGrace is how long the passive side keeps answering FIN repeats.
	CloseGrace time.Duration

	// MaxStreams bounds the number of live streams accepted on a Conn.
	// Zero selects a safe default.
	MaxStreams int
}

func (c Config) withDefaults() Config {
	if c.MaxSegment <= 0 {
		c.MaxSegment = 1024
	}
	if c.SendBuffer <= 0 {
		c.SendBuffer = 64 * 1024
	}
	if c.RecvBuffer <= 0 {
		c.RecvBuffer = 64 * 1024
	}
	if c.SendBuffer < c.MaxSegment {
		c.SendBuffer = c.MaxSegment
	}
	if c.RecvBuffer < c.MaxSegment {
		c.RecvBuffer = c.MaxSegment
	}
	if c.HandshakeTimeout <= 0 {
		c.HandshakeTimeout = 250 * time.Millisecond
	}
	if c.RTO <= 0 {
		c.RTO = 250 * time.Millisecond
	}
	if c.CloseGrace <= 0 {
		c.CloseGrace = 5 * c.RTO
	}
	if c.MaxStreams <= 0 {
		c.MaxStreams = 256
	}
	return c
}
