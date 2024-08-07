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
	logs     []LogEntry
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
		// The Leader Completeness Property guarantees that a leader has all committed entries,
		// but at the start of its term, it may not know which those are.
		// To find out, it needs to commit an entry from its term.
		// Raft handles this by having each leader commit a blank no-op entry into the log at the start of its term. ($9)
		// after 2D it serves as logical index offset.
		logs: []LogEntry{
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

// ^^^^ can access without lock ^^^^
// vvvv need lock while access vvvv

func (rf *raftState) lastLogTermAndIndex() (int32, int) {
	lastLog := rf.logs[len(rf.logs)-1]
	return lastLog.Term, rf.lastLogIndex()
}

func (rf *raftState) lastLogIndex() int {
	return rf.logs[len(rf.logs)-1].Index
}

// return the log entry at the given index
func (rf *raftState) logEntry(index int) *LogEntry {
	base := rf.baseIndex()
	if index < base || index-base >= len(rf.logs) {
		return nil
	}
	return &rf.logs[index-base]
}

func (rf *raftState) logsFrom(idx int) []LogEntry {
	base := rf.baseIndex()
	if idx < base {
		return nil
	}
	return rf.logs[idx-base:]
}

// return the log entries from start to end index (inclusive)
func (rf *raftState) logsRange(start, end int) []LogEntry {
	if start > end {
		return nil
	}
	base := rf.baseIndex()
	if start < base || end < base || start-base >= len(rf.logs) || end-base >= len(rf.logs) {
		return nil
	}
	return rf.logs[start-base : end-base+1]
}

func (rf *raftState) replaceLogs(logs []LogEntry) {
	rf.logs = logs
}

func (rf *raftState) setBaseLog(index int, term int32) {
	rf.logs[0] = LogEntry{Index: index, Term: term, Command: nil}
}

func (rf *raftState) getLastApplied() int {
	return rf.lastApplied
}

func (rf *raftState) setLastApplied(i int) int {
	rf.lastApplied = i
	return rf.lastApplied
}

func (rf *raftState) setNextIndex(nextIdx []int) {
	rf.nextIndex = nextIdx
}

func (rf *raftState) getNodeNextIndex(nodeID int) int {
	return rf.nextIndex[nodeID]
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
func (rf *raftState) appendLog(le LogEntry) (index int) {
	le.Index = rf.lastLogIndex() + 1
	rf.logs = append(rf.logs, le)
	return le.Index
}

// append the log entries to the log state starting from the given index
func (r *raftState) appendLogs(idx int, les []LogEntry) {
	base := r.baseIndex()
	if idx < base {
		panic("invalid index")
	}
	r.logs = append(r.logs[0:(idx-base)+1], les...)
}

func (rf *raftState) setCommitIndex(i int) {
	rf.commitIndex = i
}

func (rf *raftState) baseIndex() int {
	return rf.logs[0].Index
}

func (rf *raftState) getNodeEntries(nodeID int) ([]LogEntry, *LogEntry, error) {
	if len(rf.nextIndex) == 0 {
		return nil, nil, errors.New("empty nextIndex, the node is not a leader")
	}
	nextIdx := rf.nextIndex[nodeID]
	switch {
	case nextIdx <= 0:
		return nil, nil, errors.New("invalid next index")
	case nextIdx > rf.lastLogIndex()+1:
		return nil, nil, nil
	case nextIdx == rf.lastLogIndex()+1:
		return nil, rf.logEntry(nextIdx - 1), nil //
	default: // 1 <= x <= lastLogIndex
		return rf.logsFrom(nextIdx), rf.logEntry(nextIdx - 1), nil
	}
}
