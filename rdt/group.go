package rdt

// GroupStats is a point-in-time accounting snapshot for one business
// group. All byte counters attribute each segment to the group the
// sending stream belonged to when the segment was first transmitted,
// so later group switches never move earlier charges.
type GroupStats struct {
	Weight   uint64
	Sent     uint64 // bytes transmitted (first send only, retransmits excluded)
	Acked    uint64 // bytes acknowledged by the peer
	Inflight uint64 // sent but not yet acked or dropped
	Dropped  uint64 // unacked + queued bytes discarded by stream reset/cancel
}

// StreamStats is a point-in-time accounting snapshot for one stream.
type StreamStats struct {
	Group    uint32 // current group; future sends are charged to it
	Sent     uint64
	Acked    uint64
	Inflight uint64
	Dropped  uint64 // unacked + queued bytes discarded by reset/cancel
}

type groupState struct {
	id       uint32
	weight   uint64
	inflight uint64
	sent     uint64
	acked    uint64
	dropped  uint64
	cursor   int // round-robin cursor over Session.streamOrder
}

const defaultGroupID = uint32(0)

// groupLocked returns the state for id, creating it with weight 1 and no
// banked credit. A freshly created group earns quota only through its
// guaranteed share, so switching groups cannot mint extra allowance.
func (s *Session) groupLocked(id uint32) *groupState {
	if g, ok := s.groups[id]; ok {
		return g
	}
	g := &groupState{id: id, weight: 1}
	s.groups[id] = g
	s.groupOrder = append(s.groupOrder, id)
	return g
}

func (s *Session) totalWeightLocked() uint64 {
	var total uint64
	for _, g := range s.groups {
		total += g.weight
	}
	if total == 0 {
		total = 1
	}
	return total
}

// groupShareLocked is the guaranteed in-flight budget of a group: its
// weight-proportional slice of the total quota (at least one byte so a
// legal stream always progresses eventually).
func (s *Session) groupShareLocked(g *groupState) uint64 {
	share := s.totalQuota * g.weight / s.totalWeightLocked()
	if share == 0 {
		share = 1
	}
	return share
}

// quotaRoomLocked reports how many bytes the group may put in flight now.
// A group may always use spare connection quota, and may additionally
// fall back on its own guaranteed share even when the connection quota is
// exhausted by other busy groups.
func (s *Session) quotaRoomLocked(g *groupState) int64 {
	spare := int64(s.totalQuota) - int64(s.inflightTotal)
	guaranteed := int64(s.groupShareLocked(g)) - int64(g.inflight)
	room := spare
	if guaranteed > room {
		room = guaranteed
	}
	if room < 0 {
		room = 0
	}
	return room
}

// SetGroupWeight changes a group's scheduling weight at runtime. In-flight
// bytes stay charged to the group; only future sends use the new share.
// A zero weight is treated as 1 so the group can still progress.
func (s *Session) SetGroupWeight(id uint32, weight uint64) {
	if weight == 0 {
		weight = 1
	}
	s.mu.Lock()
	s.groupLocked(id).weight = weight
	s.pumpAllLocked()
	s.kick()
	s.mu.Unlock()
}

// SetTotalQuota resizes the connection-wide in-flight budget at runtime.
// Bytes already in flight keep counting against the new quota, so
// shrinking it pauses new sends until enough acknowledgements arrive;
// it never drops or re-meters in-flight data, hence a slow receiver's
// backlog stays bounded by its advertised window as before.
func (s *Session) SetTotalQuota(n uint64) {
	s.mu.Lock()
	if n < uint64(s.cfg.MaxSegment) {
		n = uint64(s.cfg.MaxSegment)
	}
	s.totalQuota = n
	s.pumpAllLocked()
	s.kick()
	s.mu.Unlock()
}

// TotalQuota returns the current connection-wide in-flight budget.
func (s *Session) TotalQuota() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.totalQuota
}

// SetStreamGroup moves a stream to a business group. Only bytes written
// after the switch are charged to the new group; bytes already queued
// keep the group of their write, and occupancy, acknowledgements and
// cancellation of earlier sends remain with the group that sent them.
func (s *Session) SetStreamGroup(streamID uint32, group uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.streams[streamID]
	if st == nil {
		return ErrClosed
	}
	s.groupLocked(group)
	st.group = group
	s.pumpAllLocked()
	return nil
}

// GroupStats returns the accounting snapshot for one group.
func (s *Session) GroupStats(id uint32) GroupStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	g := s.groups[id]
	if g == nil {
		return GroupStats{}
	}
	return GroupStats{
		Weight:   g.weight,
		Sent:     g.sent,
		Acked:    g.acked,
		Inflight: g.inflight,
		Dropped:  g.dropped,
	}
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
		Group:    st.group,
		Sent:     st.statSent,
		Acked:    st.statAcked,
		Inflight: st.sndNxt - st.sndUna,
		Dropped:  st.statDropped,
	}, true
}

// chargeSendLocked meters a first-time data transmission to the group the
// segment was pinned to when its bytes were written.
func (s *Session) chargeSendLocked(st *streamState, seg *outSeg) {
	g := s.groupLocked(seg.group)
	n := uint64(len(seg.data))
	g.inflight += n
	g.sent += n
	s.inflightTotal += n
	st.statSent += n
}

// releaseSegLocked settles an acknowledged segment against the group it
// was charged to at send time.
func (s *Session) releaseSegLocked(st *streamState, seg *outSeg) {
	g := s.groupLocked(seg.group)
	n := uint64(len(seg.data))
	g.inflight -= n
	g.acked += n
	s.inflightTotal -= n
	st.statAcked += n
}

// dropStreamLocked discards queued and unacked bytes (reset/cancel),
// charging each dropped byte to the group it was pinned to: in-flight
// segments settle against the group that transmitted them, while queued
// chunks were never charged to any group's in-flight total, so they only
// add to the dropped ledger of the group active when they were written.
// Keeping the two kinds of drop separate avoids double counting.
func (s *Session) dropStreamLocked(st *streamState) {
	for _, seg := range st.segments {
		g := s.groupLocked(seg.group)
		n := uint64(len(seg.data))
		g.inflight -= n
		g.dropped += n
		s.inflightTotal -= n
		st.statDropped += n
	}
	st.segments = make(map[uint64]*outSeg)
	for _, c := range st.queued {
		g := s.groupLocked(c.group)
		n := uint64(len(c.data))
		g.dropped += n
		st.statDropped += n
	}
	st.queued = nil
}
