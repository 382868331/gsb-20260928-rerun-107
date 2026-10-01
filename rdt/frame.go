package rdt

// FrameKind selects the role of a Frame exchanged on an unreliable transport.
type FrameKind uint8

const (
	KindSyn FrameKind = iota + 1
	KindSynAck
	KindData
	KindAck
	KindFin
	KindFinAck
	KindRst
)

// Frame is the wire-level message. Epoch identifies the connection
// incarnation; frames from a previous incarnation are discarded.
type Frame struct {
	Kind  FrameKind
	Epoch uint64
	// Stream identifies the independent bidirectional byte stream inside
	// one connection. Stream 1 is the bootstrap stream used by Session.
	Stream uint32

	// Data frames: Seq is the first byte; the count is len(Payload).
	// Ack frames: Ack is the next expected byte (cumulative).
	Seq uint64
	Ack uint64

	// Receiver advertised flow-control window in bytes. Only meaningful
	// on KindAck (and handshake/fin acknowledgements).
	Window uint64

	Payload []byte
}

func (f Frame) length() uint64 { return uint64(len(f.Payload)) }

// Transport is a bidirectional unreliable datagram channel. Recv may
// block, but the implementation must be safe to Close while blocked.
type Transport interface {
	Send(f Frame) error
	Recv() (Frame, error)
	Close() error
}
