package rdt

import "sort"

// heldSeg is an out-of-order received byte interval.
type heldSeg struct {
	seq  uint64
	data []byte
}

func (h heldSeg) end() uint64 { return h.seq + uint64(len(h.data)) }

func holdBytes(hold []heldSeg) uint64 {
	v := uint64(0)
	for _, h := range hold {
		v += uint64(len(h.data))
	}
	return v
}

// insertHeld merges a new segment into an ordered, disjoint slice and
// returns the slice plus the number of genuinely NEW bytes stored.
// Overlapping/duplicate bytes from retransmits are spliced out, so
// delivery never repeats.
func insertHeld(hold []heldSeg, seq uint64, data []byte) ([]heldSeg, uint64) {
	if len(data) == 0 {
		return hold, 0
	}
	before := holdBytes(hold)
	hold = append(hold, heldSeg{seq: seq, data: append([]byte(nil), data...)})
	sort.SliceStable(hold, func(i, j int) bool { return hold[i].seq < hold[j].seq })

	out := make([]heldSeg, 0, len(hold))
	for _, cur := range hold {
		if len(out) == 0 || cur.seq > out[len(out)-1].end() {
			out = append(out, cur)
			continue
		}
		last := &out[len(out)-1]
		if cur.end() <= last.end() {
			continue // fully contained duplicate
		}
		off := last.end() - cur.seq
		if off > uint64(len(cur.data)) {
			off = uint64(len(cur.data))
		}
		last.data = append(last.data, cur.data[off:]...)
	}
	return out, holdBytes(out) - before
}
