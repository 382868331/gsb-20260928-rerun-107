package rdt

import "encoding/binary"

// Packet flags.
const (
	flagSYN uint8 = 1 << iota
	flagACK
	flagFIN
	flagRST
)

// Packet is the unit exchanged on a Transport. Seq/Ack are byte-oriented
// absolute offsets. Epoch identifies the connection instance; packets from a
// previous incarnation must never be accepted by a newer session.
type Packet struct {
	Epoch uint64
	Seq   uint64
	Ack   uint64
	Flags uint8
	Data  []byte
}

func (p Packet) has(f uint8) bool { return p.Flags&f != 0 }

// Marshal serializes a packet. SYN/FIN occupy one logical byte of sequence
// space, exactly like TCP, so they are acknowledged independently of data.
func (p Packet) Marshal() []byte {
	hdr := make([]byte, 8+8+8+1)
	binary.BigEndian.PutUint64(hdr[0:8], p.Epoch)
	binary.BigEndian.PutUint64(hdr[8:16], p.Seq)
	binary.BigEndian.PutUint64(hdr[16:24], p.Ack)
	hdr[24] = p.Flags
	return append(hdr, p.Data...)
}

// ParsePacket decodes a packet, returning false if the frame is truncated.
func ParsePacket(b []byte) (Packet, bool) {
	if len(b) < 25 {
		return Packet{}, false
	}
	return Packet{
		Epoch: binary.BigEndian.Uint64(b[0:8]),
		Seq:   binary.BigEndian.Uint64(b[8:16]),
		Ack:   binary.BigEndian.Uint64(b[16:24]),
		Flags: b[24],
		Data:  append([]byte(nil), b[25:]...),
	}, true
}
