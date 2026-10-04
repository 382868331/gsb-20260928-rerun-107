package rdt

import "sort"

// qChunk is a run of queued bytes attributed to the group that was
// current on the stream when the bytes were accepted. Switching groups
// later never re-attributes already queued or in-flight bytes.
type qChunk struct {
	group uint32
	data  []byte
}

// groupState is a business group sharing this connection's send quota.
// Weighted deficit round-robin between groups gives heavier (interactive)
// groups priority while guaranteeing lighter groups keep moving.
type groupState struct {
	id      uint32
	weight  uint32
	deficit uint64
	cursor  int

	sent uint64 // bytes handed to the transport (first send only)
	pend uint64 // bytes sent but not yet cumulatively acked
	drop uint64 // queued/unacked bytes discarded by reset or abort
}

// GroupStats is a point-in-time accounting snapshot for one group.
type GroupStats struct {
	Group   uint32
	Weight  uint32
	Sent    uint64
	Pending uint64
	Dropped uint64
}

// StreamStats is a point-in-time accounting snapshot for one stream.
type StreamStats struct {
	Stream  uint32
	Group   uint32
	Sent    uint64
	Pending uint64
	Dropped uint64
}

func (s *Session) groupLocked(id uint32) *groupState {
	g := s.groups[id]
	if g == nil {
		g = &groupState{id: id, weight: 1}
		s.groups[id] = g
	}
	return g
}

// SetGroupWeight creates or updates a business group's scheduling weight.
// Weight 0 is treated as 1. Safe to call while transfers are running.
func (s *Session) SetGroupWeight(id uint32, weight uint32) {
	if weight == 0 {
		weight = 1
	}
	s.mu.Lock()
	s.groupLocked(id).weight = weight
	s.kick()
	s.mu.Unlock()
}

// SetTotalQuota changes the connection-wide cap on unacknowledged bytes.
// Bytes already in flight keep counting against the new quota, so lowering
// it merely pauses new sends until the pipeline drains; it never pushes a
// slow receiver's backlog past its advertised window. Zero restores the
// configured default.
func (s *Session) SetTotalQuota(n uint64) {
	s.mu.Lock()
	if n == 0 {
		n = uint64(s.cfg.GroupQuota)
	}
	s.totalQuota = n
	s.pumpAllLocked()
	s.kick()
	s.mu.Unlock()
}

// SetGroup moves the default stream to a business group. Only bytes
// written after the switch are attributed to the new group.
func (s *Session) SetGroup(group uint32) { s.setStreamGroup(defaultStreamID, group) }

func (s *Session) setStreamGroup(id uint32, group uint32) {
	s.mu.Lock()
	if st := s.streams[id]; st != nil {
		st.group = group
		s.groupLocked(group)
	}
	s.mu.Unlock()
}

// GroupStats returns accounting snapshots for every known group, ordered
// by group id.
func (s *Session) GroupStats() []GroupStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]GroupStats, 0, len(s.groups))
	for _, g := range s.groups {
		out = append(out, GroupStats{
			Group:   g.id,
			Weight:  g.weight,
			Sent:    g.sent,
			Pending: g.pend,
			Dropped: g.drop,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Group < out[j].Group })
	return out
}

// StreamStats returns the accounting snapshot for one stream.
func (s *Session) StreamStats(id uint32) (StreamStats, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.streams[id]
	if st == nil {
		return StreamStats{}, false
	}
	return StreamStats{
		Stream:  id,
		Group:   st.group,
		Sent:    st.statSent,
		Pending: st.statPend,
		Dropped: st.statDrop,
	}, true
}

// orderedGroupsLocked returns groups sorted for scheduling: heavier
// (interactive) groups first, ties by id.
func (s *Session) orderedGroupsLocked() []*groupState {
	out := make([]*groupState, 0, len(s.groups))
	for _, g := range s.groups {
		out = append(out, g)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].weight != out[j].weight {
			return out[i].weight > out[j].weight
		}
		return out[i].id < out[j].id
	})
	return out
}

// nextGroupStreamLocked round-robins streams within one group.
func (s *Session) nextGroupStreamLocked(g *groupState) (uint32, *streamState) {
	n := len(s.streamOrder)
	for i := 0; i < n; i++ {
		idx := (g.cursor + i) % n
		st := s.streams[s.streamOrder[idx]]
		if st != nil && st.group == g.id {
			g.cursor = (idx + 1) % n
			return st.id, st
		}
	}
	return 0, nil
}

// discardStreamSendLocked drops all queued and unacked bytes of a stream,
// attributing the loss to the groups the bytes were sent under.
func (s *Session) discardStreamSendLocked(st *streamState) {
	for _, ch := range st.queued {
		g := s.groupLocked(ch.group)
		g.drop += uint64(len(ch.data))
		st.statDrop += uint64(len(ch.data))
	}
	st.queued = nil
	for seq, seg := range st.segments {
		n := uint64(len(seg.data))
		g := s.groupLocked(seg.group)
		g.pend -= n
		g.drop += n
		st.statPend -= n
		st.statDrop += n
		s.inFlight -= n
		delete(st.segments, seq)
	}
}
