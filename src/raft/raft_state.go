package raft

import (
	"sync/atomic"
)

type RaftState uint32

const (
	Follower RaftState = iota
	Candidate
	Leader
	Shutdown
)

const (
	voteForNull = -1
)

func (s RaftState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// raftState is used to maintain various state variables
// and provides an interface to set/get the variables in a
// thread safe manner.
type raftState struct {
	state       RaftState
	currentTerm int32 //  Terms act as a logical clock
	dead        int32 // set by Kill()

	votedFor int32 // candidateId that received vote in current term (or null if none)
	Log      []LogEntry
}

func newRaftState() raftState {
	return raftState{
		state:       Follower,
		currentTerm: 0,
		dead:        0,
		votedFor:    voteForNull,
		Log:         nil,
	}
}

// get the current state of the raft node
// use atomic to avoid uinsg locks
func (r *raftState) getState() RaftState {
	stateAddr := (*uint32)(&r.state)
	return RaftState(atomic.LoadUint32(stateAddr))
}

func (r *raftState) setState(s RaftState) {
	stateAddr := (*uint32)(&r.state)
	atomic.StoreUint32(stateAddr, uint32(s))
}
func (r *raftState) getCurrentTerm() int32 {
	return atomic.LoadInt32(&r.currentTerm)
}
func (r *raftState) setCurrentTerm(term int32) {
	atomic.StoreInt32(&r.currentTerm, term)
}
func (r *raftState) addCurrentTerm() int32 {
	return atomic.AddInt32(&r.currentTerm, 1)
}
func (r *raftState) getVotedFor() int32 {
	return atomic.LoadInt32(&r.votedFor)
}
func (r *raftState) setVotedFor(val int32) {
	atomic.StoreInt32(&r.votedFor, val)
}

func (rf *raftState) lastLogTermAndIndex() (int, int) {
	// TODO: implement this when log is implemented
	return 0, -1
}

// dead
func (r *raftState) killed() bool {
	z := atomic.LoadInt32(&r.dead)
	return z == 1
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (r *raftState) Kill() {
	atomic.StoreInt32(&r.dead, 1)
}
