// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	initialElectionTimeout int
	// a randome timeout based on initialelectionTimeout
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	// initialize a RaftLog
	raftLog := newLog(c.Storage)
	hardState, confState, _ := c.Storage.InitialState()

	// initialize a Raft
	raft := Raft{id: c.ID, Term: hardState.Term, Vote: hardState.Vote, heartbeatTimeout: c.HeartbeatTick,
		initialElectionTimeout: c.ElectionTick, electionTimeout: c.ElectionTick + rand.Intn(c.ElectionTick),
		RaftLog: raftLog, Prs: make(map[uint64]*Progress)}
	
	// load peers information
		peers := c.peers
	if len(confState.Nodes) > 0 {
		peers = confState.Nodes
	}
	for _, id := range peers {
		raft.Prs[id] = &Progress{Match: 0, Next: 1}
	}

	return &raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	// get index and logterm of last entry
	index, lterm := r.Prs[to].Match, uint64(0)
	if index > 0 {
		lterm = r.RaftLog.entries[index-1].Term
	}

	// get entries to be sent
	entries := []*pb.Entry{}
	for i := int(index); i < len(r.RaftLog.entries); i++ {
		entries = append(entries, &(r.RaftLog.entries[i]))
	}

	// send message
	msg_append := pb.Message{From: r.id, To: to, Term: r.Term, MsgType: pb.MessageType_MsgAppend,
		Index: index, LogTerm: lterm, Entries: entries, Commit: r.RaftLog.committed}
	r.msgs = append(r.msgs, msg_append)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	// heartbest with index and logterm as 0
	msg_hb := pb.Message{From: r.id, To: to, Term: r.Term, MsgType: pb.MessageType_MsgHeartbeat, Commit: r.RaftLog.committed}
	r.msgs = append(r.msgs, msg_hb)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	// leader sends a heartbest per heartbeastTimeout, 
	// candidate and follower start a leader election with a electionTimeout.
	if r.State == StateLeader {
		r.heartbeatElapsed += 1
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			for peerid := range r.Prs {
				if peerid != r.id {
					r.sendHeartbeat(peerid)
				}
			}
		}
	} else if r.State == StateCandidate || r.State == StateFollower {
		r.electionElapsed += 1
		if r.electionElapsed >= r.electionTimeout {
			index, lterm := uint64(0), uint64(0)
			if len(r.RaftLog.entries) > 0 {
				e := r.RaftLog.entries[len(r.RaftLog.entries)-1]
				index, lterm = e.Index, e.Term
			}
			r.Step(pb.Message{From: r.id, To: r.id, Term: r.Term, Index: index, LogTerm: lterm, MsgType: pb.MessageType_MsgHup})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.votes = make(map[uint64]bool)
	r.State, r.votes[r.id], r.electionElapsed, r.electionTimeout, r.Term = StateCandidate, true, 0, r.initialElectionTimeout+rand.Intn(r.initialElectionTimeout), r.Term+1
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	if r.State == StateCandidate {
		// reset state
		r.State, r.electionElapsed = StateLeader, 0

		// append new entry
		index := uint64(len(r.RaftLog.entries) + 1)
		// e := pb.Entry{Index: index, Term: r.Term}
		r.RaftLog.entries, r.Prs[r.id].Match, r.Prs[r.id].Next =
			append(r.RaftLog.entries, pb.Entry{Index: index, Term: r.Term}), index, index+1
		if len(r.Prs) == 1 {
			r.RaftLog.committed = r.RaftLog.committed + 1
		}

		// broadcast new entry
		for peerid := range r.Prs {
			if peerid != r.id {
				r.sendAppend(peerid)
			}
		}
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// process state transition
	increTerm := false
	switch r.State {
	case StateFollower:
		if m.Term > r.Term {
			r.Term = m.Term
			increTerm = true
		}
	case StateCandidate:
		if m.Term > r.Term {
			r.State = StateFollower
			r.Term = m.Term
			increTerm = true
		}
	case StateLeader:
		if m.Term > r.Term {
			r.State = StateFollower
			r.Term = m.Term
			increTerm = true
		}
	}

	// handle message
	switch m.MsgType {
	// (follower or candidate) start a leader election when receiving a MsgHup
	case pb.MessageType_MsgHup:
		if r.State == StateFollower || r.State == StateCandidate {
			// become a candidate
			r.becomeCandidate()

			// broadcast voterequestMsg
			index, lterm := uint64(0), uint64(0)
			if len(r.RaftLog.entries) > 0 {
				e := r.RaftLog.entries[len(r.RaftLog.entries)-1]
				index, lterm = e.Index, e.Term
			}
			for peerid := range r.Prs {
				if peerid == r.id {
					r.votes[r.id] = true
					continue
				}
				msg := pb.Message{From: r.id, To: peerid, Term: r.Term, Index: index, LogTerm: lterm, MsgType: pb.MessageType_MsgRequestVote}
				r.msgs = append(r.msgs, msg)
			}
			if len(r.Prs) == 1 {
				r.becomeLeader()
			}
		}
	// (all memebers) response to leader election when receiving a MsgRequestVote
	case pb.MessageType_MsgRequestVote:
		// get latest term and log of local server
		index, lterm := uint64(0), uint64(0)
		if len(r.RaftLog.entries) > 0 {
			e := r.RaftLog.entries[len(r.RaftLog.entries)-1]
			index, lterm = e.Index, e.Term
		}
		reject := true

		// validate vote
		if r.State == StateLeader || r.State == StateCandidate { // local server has equal or higher term
			reject = true
		} else if r.State == StateFollower { // follower or leader and candidate with less term
			// determine whether vote
			if increTerm { // Candidata with larger term
				if lterm > m.LogTerm || (lterm == m.LogTerm && index > m.Index) { // local server has later log
					reject = true
				} else {
					reject = false
					r.Vote = m.From
				}
			} else if m.Term == r.Term {
				if lterm > m.LogTerm || (lterm == m.LogTerm && index > m.Index) { // local server has later log
					reject = true
				} else if r.Vote == None || r.Vote == m.From { // Candidate with same term
					reject = false
					r.Vote = m.From
				}
			}
		}

		// response to vote
		resp := pb.Message{From: r.id, To: m.From, Term: r.Term, Index: index, LogTerm: lterm, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: reject}
		r.msgs = append(r.msgs, resp)
	// (candidate) collect votes when receiving a MsgRequestVoteResponse
	case pb.MessageType_MsgRequestVoteResponse:
		if r.State == StateCandidate {
			if m.Term == r.Term {
				r.votes[m.From] = !m.Reject
				agrees := 0
				for _, vote := range r.votes {
					if vote {
						agrees += 1
					}
				}
				if agrees > len(r.Prs)/2 {
					r.becomeLeader()
				} else if len(r.votes)-agrees > len(r.Prs)/2 {
					r.State = StateFollower
				}
			}
		}
	// (leader) send a hearbeat when receiving a MsgBeat
	case pb.MessageType_MsgBeat:
		if r.State == StateLeader {
			for peerid := range r.Prs {
				if peerid == r.id {
					continue
				}
				r.sendHeartbeat(peerid)
			}
		}
	// (Candidate and Follower) handle heartbeat from leader
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	// (Leader) receive response to heartbeat from Candidate or Follower
	case pb.MessageType_MsgHeartbeatResponse:
		if r.State == StateLeader {
			mindex := int(m.Index)
			if mindex == 0 {
				r.Prs[m.From].Match, r.Prs[m.From].Next = uint64(0), uint64(1)
				r.sendAppend(m.From)
			} else if mindex <= len(r.RaftLog.entries) && r.RaftLog.entries[mindex-1].Term == m.LogTerm {
				r.Prs[m.From].Match, r.Prs[m.From].Next = uint64(mindex), uint64(mindex+1)
				if mindex < len(r.RaftLog.entries) {
					r.sendAppend(m.From)
				}
			}

		}
	// (Leader) send append requeset after receving propose request
	case pb.MessageType_MsgPropose:
		switch r.State {
		case StateLeader:
			// appendEntry and bcastAppend
			for _, e := range m.Entries {
				e.Index, e.Term = (uint64)(len(r.RaftLog.entries)+1), r.Term
				r.RaftLog.entries = append(r.RaftLog.entries, *e)
				r.Prs[r.id].Match = r.Prs[r.id].Next
				r.Prs[r.id].Next += 1
				for peerid := range r.Prs {
					if peerid != r.id {
						r.sendAppend(peerid)
					}
				}
			}
			if len(r.Prs) == 1 {
				r.RaftLog.committed += uint64(len(m.Entries))
			}
		// case StateFollower:
		// 	r.msgs = append(r.msgs, m)
		}
	// (Candidate and Follower) append entry to log when receiving a MsgAppend
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	// (Leader) receive response to append request from Candidate or Follower
	case pb.MessageType_MsgAppendResponse:
		from := m.From
		if m.Index >= r.Prs[from].Next {
			// update progress
			r.Prs[from].Match, r.Prs[from].Next = m.Index, m.Index+1

			// stats append response and update commit
			if int(r.RaftLog.committed) < len(r.RaftLog.entries) {
				successes := 0
				for _, pr := range r.Prs {
					if pr.Match >= r.RaftLog.committed+1 {
						successes += 1
					}
				}

				// leader can only commit entry (and its preceding entries) owning the same term with it
				if successes > len(r.Prs)/2 && r.RaftLog.entries[m.Index-1].Term == r.Term {
					r.RaftLog.committed = m.Index
					for id := range r.Prs {
						if r.id != id {
							r.sendAppend(id)
						}
					}
				}
			}
		}
	}

	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	reject := true
	// r.Term >= m.Term. If m.Term < r.Term, just reject.
	if r.Term == m.Term {
		// translate Candidate to Follower
		if r.State != StateLeader {
			r.State, r.Lead = StateFollower, m.From
		}

		// lastIndex is used to update r.RaftLog.committed
		lastIndex := uint64(0)

		// check preceding entries' consistence
		if int(m.Index) <= len(r.RaftLog.entries) {
			if m.Index == 0 {
				if m.LogTerm == 0 {
					reject = false
				}
			} else if r.RaftLog.entries[m.Index-1].Term == m.LogTerm {
				reject = false
				lastIndex = m.Index
			}
		}

		// preceding entries are consistent
		if !reject {
			conflict := 0
			for _, e := range m.Entries {
				e_i := int(e.Index)
				// check new entries' consistence
				if conflict == 0 {
					// local server has no entries or only has preceding entries
					if len(r.RaftLog.entries) == 0 || e_i == len(r.RaftLog.entries)+1 {
						conflict = 2
					// found conflict entries
					} else if !(e.Term == r.RaftLog.entries[e_i-1].Term) {
						conflict = 1
					// not fount conflict entries, no need to update, check next one
					} else {
						lastIndex = uint64(e_i)
					}
				}

				// remove conflict and afterward entries
				if conflict == 1 {
					conflict = 2
					r.RaftLog.entries = r.RaftLog.entries[0 : e.Index-1]
					if r.RaftLog.stabled > e.Index-1 {
						r.RaftLog.stabled = e.Index - 1
					}
				}

				// append new entries
				if conflict == 2 {
					lastIndex += 1
					r.RaftLog.entries = append(r.RaftLog.entries, *e)
				}
			}

			// update committed 
			// if synchronized entries is preceding to local latest entry, update commit and remove 
			// afterward entries from RaftLog and storage
			if lastIndex < r.RaftLog.committed {
				// update commmit
				r.RaftLog.committed = lastIndex

				// remove afterward from RaftLog
				r.RaftLog.entries = r.RaftLog.entries[:lastIndex]

				// remove afterward from storage
				if r.RaftLog.stabled > r.RaftLog.committed {
					r.RaftLog.stabled = r.RaftLog.committed
				}
				// select smaller of m.Commit and lastIndex to be new commit
			} else if m.Commit >= r.RaftLog.committed {
				if m.Commit < lastIndex {
					r.RaftLog.committed = m.Commit
				} else {
					r.RaftLog.committed = lastIndex
				}
			}
		}

	}
	msg_appendresp := pb.Message{From: r.id, To: m.From, Term: r.Term, Index: m.Index + uint64(len(m.Entries)), MsgType: pb.MessageType_MsgAppendResponse, Reject: reject}
	r.msgs = append(r.msgs, msg_appendresp)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if r.State != StateLeader {
		index, lterm := uint64(0), uint64(0)
		if len(r.RaftLog.entries) > 0 {
			e := r.RaftLog.entries[len(r.RaftLog.entries)-1]
			index, lterm = e.Index, e.Term
		}
		msg_hbresp := pb.Message{From: r.id, To: m.From, Term: r.Term, Index: index, LogTerm: lterm,
			MsgType: pb.MessageType_MsgHeartbeatResponse, Commit: r.RaftLog.committed}
		r.msgs = append(r.msgs, msg_hbresp)
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
