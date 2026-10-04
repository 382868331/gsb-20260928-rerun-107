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
	// MaxStreams bounds how many remotely initiated streams may exist at once.
	MaxStreams int
	// GroupQuota bounds total unacknowledged (in-flight) data bytes across
	// all streams of the connection. Business groups share it in proportion
	// to their weights; every group keeps a guaranteed weighted share so a
	// busy priority group cannot starve the others. Defaults to SendBuffer.
	GroupQuota int
	// HandshakeTimeout bounds a single handshake step retransmission wait.
	HandshakeTimeout time.Duration
	// RTO is the retransmission timeout for data and FIN segments.
	RTO time.Duration
	// CloseGrace is how long the passive side keeps answering FIN repeats.
	CloseGrace time.Duration
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
	if c.GroupQuota <= 0 {
		c.GroupQuota = c.SendBuffer
	}
	if c.GroupQuota < c.MaxSegment {
		c.GroupQuota = c.MaxSegment
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
	return c
}
