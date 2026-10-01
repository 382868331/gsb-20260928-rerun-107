package rdt

import "errors"

var (
	// ErrClosed is returned after the connection closed or was cancelled.
	ErrClosed = errors.New("rdt: session closed")
	// ErrHandshake means the handshake did not complete in time.
	ErrHandshake = errors.New("rdt: handshake failed")
)
