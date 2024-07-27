package raft

import (
	"errors"
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
	// index of highest log entry known to be committed (initialized to 0, increases monotonically)
	commitIndex int

	// index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	lastApplied int

	// Volatile state on leaders:
	// next log index to replicate to follower
	//  (initialized to leader last log index + 1)
	nextIndex []int
	// for each server, index of highest log entry known to be replicated on server
	// (initialized to 0, increases monotonically)
	matchIndex []int
}

func newRaftState() raftState {
	return raftState{
		state:       Follower,
		currentTerm: 0,
		dead:        0,
		votedFor:    voteForNull,
		// keep a dummy entry
		Log: []LogEntry{
			{Term: 0, Index: 0, Command: nil},
		},
		nextIndex:   nil, // nil for follower
		commitIndex: 0,
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

func (rf *raftState) lastLogTermAndIndex() (int32, int) {
	if len(rf.Log) > 0 {
		return rf.Log[len(rf.Log)-1].Term, len(rf.Log) - 1
	}
	return 0, -1
}

func (rf *raftState) setNextIndex(nextIdx []int) {
	rf.nextIndex = nextIdx
}

func (rf *raftState) setNodeNextIndex(nodeID, nextIdx int) error {
	if len(rf.nextIndex) == 0 {
		return errors.New("empty nextIndex")
	}
	if nodeID < 0 || nodeID >= len(rf.nextIndex) {
		return errors.New("invalid node ID")
	}
	if nextIdx <= 0 {
		return errors.New("invalid next index")
	}
	rf.nextIndex[nodeID] = nextIdx
	return nil
}

func (rf *raftState) setMatchIndex(matchIdx []int) {
	rf.matchIndex = matchIdx
}

func (rf *raftState) setNodeMatchIndex(nodeID, matchIdx int) error {
	if len(rf.matchIndex) == 0 {
		return errors.New("empty matchIndex")
	}
	if nodeID < 0 || nodeID >= len(rf.matchIndex) {
		return errors.New("invalid node ID")
	}
	rf.matchIndex[nodeID] = matchIdx
	return nil
}

// append the log entry to the log state and return the appended index
func (r *raftState) appendLog(le LogEntry) (index int) {
	le.Index = len(r.Log)
	r.Log = append(r.Log, le)
	return len(r.Log) - 1
}

// append the log entry to the log state and return the appended index
func (r *raftState) appendLogs(idx int, les []LogEntry) {
	r.Log = append(r.Log[0:idx+1], les...)
}

func (rf *raftState) setCommitIndex(i int) {
	rf.commitIndex = i
}

// If commitIndex > lastApplied: increment lastApplied,
// apply log[lastApplied] to state machine (§5.3)
func (rf *raftState) applyMsgs(applyCh chan ApplyMsg) {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.Log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
		applyCh <- msg
	}
}

func (rf *raftState) getNodeEntries(nodeID int) ([]LogEntry, *LogEntry) {
	nextIdx := rf.nextIndex[nodeID]
	if nextIdx >= len(rf.Log) {
		if nextIdx == len(rf.Log) && len(rf.Log) > 0 {
			return nil, &rf.Log[len(rf.Log)-1]
		}
		return nil, nil
	}
	if nextIdx > 0 {
		return rf.Log[nextIdx:], &rf.Log[nextIdx-1]
	}
	return rf.Log[nextIdx:], nil
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
