package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/lablog"
	"6.824/labrpc"
	"6.824/labutil"
)

// 6.824: The tester requires that the leader send heartbeat RPCs no more than ten times per second.
const (
	leaderHeartbeatIntervalMs = 100
	// Raft uses randomized election timeouts to ensure that split votes are rare and that they are resolved quickly.
	// To prevent split votes in the first place, election timeouts are chosen randomly from a fixed interval.
	// Make sure the election timeouts in different peers don't always fire at the same time,
	// or else all peers will vote only for themselves and no one will become the leader.
	electinTimeoutMinMs = 600
	electinTimeoutMaxMs = 1000
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Index   int
	Term    int32
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	// dead      int32             // moved to raft state

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	raftState

	// channels for state machine
	stepDownCh    chan bool
	winElectionCh chan bool
	heartbeatCh   chan bool
	applyCh       chan ApplyMsg

	applyCond *sync.Cond // to wake up apply goroutine
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return int(rf.getCurrentTerm()), rf.getState() == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// Persistent state on all servers: currentTerm, votedFor, log[]
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	rf.persister.SaveRaftState(rf.encodeState())
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.getCurrentTerm())
	e.Encode(rf.getVotedFor())
	e.Encode(rf.logs)
	return w.Bytes()
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currTerm int32
	var votedFor int32
	var logs []LogEntry

	if d.Decode(&currTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		lablog.Debug(rf.me, lablog.Error, "readPersist failed")
	} else {
		rf.setCurrentTerm(currTerm)
		rf.setVotedFor(votedFor)
		rf.logs = logs
	}
	// what happens on crash+restart?
	// service reads snapshot from disk Raft reads persisted log from disk
	// service tells Raft to set lastApplied to last included index
	// to avoid re-applying already-applied log entries
	// ref: http://nil.csail.mit.edu/6.824/2021/notes/l-raft2.txt
	rf.lastApplied = rf.baseIndex()
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	lablog.Debug(rf.me, lablog.Snapshot, "cond install snapshot at term %d, index %d", lastIncludedTerm, lastIncludedIndex)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if lastIncludedIndex < rf.commitIndex {
		lablog.Debug(rf.me, lablog.Snapshot, "snapshot too old, index=%d is already committed", lastIncludedIndex)
		return false
	}
	if lastIncludedIndex > rf.baseIndex() {
		// Usually the snapshot will contain new information not already in the recipient’s log.
		// In this case, the follower discards its entire log ($7)
		newLogs := make([]LogEntry, 1)
		rf.replaceLogs(newLogs)
		rf.setBaseLog(lastIncludedIndex, int32(lastIncludedTerm))
	} else {
		// If instead the follower receives a snapshot that describes a prefix of its log (due to retransmission or by mistake),
		// then log entries covered by the snapshot are deleted but entries following the snapshot are still valid and must be retained. ($7)
		rf.trimLogs(lastIncludedIndex, int32(lastIncludedTerm))
	}
	rf.setLastApplied(lastIncludedIndex)
	rf.setCommitIndex(lastIncludedIndex)
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	lablog.Debug(rf.me, lablog.Snapshot, "snapshot at index %d", index)
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.baseIndex() {
		lablog.Debug(rf.me, lablog.Snapshot, "snapshot index=%d is already compacted", index)
		return
	}
	if index > rf.lastLogIndex() {
		lablog.Debug(rf.me, lablog.Snapshot, "snapshot index=%d is out of range", index)
		return
	}
	offsetTerm := rf.logEntry(index).Term
	rf.trimLogs(index, offsetTerm)
	rf.setBaseLog(index, offsetTerm)
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
	lablog.Debug(rf.me, lablog.Snapshot, "snapshot index=%d saved and base index, term updated to %d, %d", index, rf.baseIndex(), offsetTerm)
}

// trimLogs trims the log entries from the given index and term.
func (rf *Raft) trimLogs(index int, term int32) {
	trimmed := make([]LogEntry, 0)
	for i := rf.lastLogIndex(); i >= rf.baseIndex(); i-- {
		entry := rf.logEntry(i)
		if entry.Term == term && entry.Index == index {
			trimmed = append(trimmed, rf.logsFrom(i)...)
			rf.replaceLogs(trimmed)
			return
		}
	}
	panic("not found")
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int32
	CandidateId  int32
	LastLogIndex int
	LastLogTerm  int32
	LeaderCommit int // leader’s commitIndex
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int32 // current term
	VoteGranted bool
}

// example RequestVote RPC handler.
// on receive RequestVote from peer
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	lablog.Debug(rf.me, lablog.Vote, "received vote req %+v", *args)
	rf.mu.Lock()
	// Setup a response
	currTerm := rf.getCurrentTerm()
	reply.Term = currTerm
	reply.VoteGranted = false

	// receiving outdated vote req || should not send vote request to node itself
	if args.Term < currTerm || args.CandidateId == int32(rf.me) || rf.killed() {
		rf.mu.Unlock()
		return
	}
	// becomes follower and reset votefor
	if args.Term > currTerm {
		rf.becomeFollower(args.Term)
	}

	votedFor := rf.getVotedFor()
	canVoteForThisCandidate := (votedFor == voteForNull || votedFor == args.CandidateId)
	// prevents a candidate with an outdated log from becoming leader.
	ismoreUpToDate := rf.isMoreUpToDate(args.LastLogTerm, args.LastLogIndex)
	if canVoteForThisCandidate && ismoreUpToDate {
		lablog.Debug(rf.me, lablog.Vote, "grant vote to candidate=%d at Term %d", args.CandidateId, args.Term)
		reply.VoteGranted = true
		// follower need to remember which candidate it voted
		// persist votedFor to avoid voting twice within one term
		rf.setVotedFor(args.CandidateId)
	}
	rf.persist()
	rf.mu.Unlock()
	// IMPORTANT: to reset election timeout
	// otherwise, the follower will frequently timeout and start a new election
	rf.heartbeatCh <- true
}

// isMoreUpToDate determines if the given (term, lastIndex) log is more up-to-date by comparing the index
// and term of the last entries in the existing logs.
// If the logs have last entries with different terms, then the log with the later term is more up-to-date.
// If the logs end with the same term, then whichever log has the larger lastIndex is more up-to-date.
// If the logs are the same, the given log is up-to-date.
func (rf *Raft) isMoreUpToDate(reqTerm int32, reqIdx int) bool {
	lastLogTerm, lastLogIdx := rf.lastLogTermAndIndex()
	if reqTerm != lastLogTerm {
		return reqTerm > lastLogTerm
	}
	// if the terms are the same and the candidate’s log contains at least as many entries as the
	// recipient’s log.
	return reqIdx >= lastLogIdx
}

// becomeFollower changes the state of the Raft server to Follower.
// only call while holding the lock
func (rf *Raft) becomeFollower(term int32) {
	lablog.Debug(rf.me, lablog.Info, "become follower at term=%d", term)
	rf.resetChs()
	prevState := rf.getState()
	rf.setState(Follower)
	rf.setVotedFor(voteForNull)
	rf.setCurrentTerm(term)
	rf.setNextIndex(nil)  // leader only state
	rf.setMatchIndex(nil) // leader only state
	if prevState != Follower {
		rf.stepDownCh <- true
	}
}

// request vote which save response grant to chan
// BEWARE: there are mulitple goroutines calling requestVote
func (rf *Raft) requestVote(peer int, args *RequestVoteArgs, grantCount *uint32) {
	lablog.Debug(rf.me, lablog.Vote, "requestVote to peer %d: %+v", peer, args)
	if rf.invalidCandidateState(args.Term) {
		lablog.Debug(rf.me, lablog.Vote, "early exit")
		return
	}

	reply := &RequestVoteReply{}
	sendOk := rf.sendRequestVote(peer, args, reply)
	lablog.Debug(rf.me, lablog.Vote, "received vote reply from node %d: %+v", peer, reply)

	if !sendOk {
		lablog.Debug(rf.me, lablog.Vote, "vote request of term %d to %d was dropped", args.Term, peer)
		return
	}
	// cancel election
	if reply.Term > args.Term {
		rf.mu.Lock()
		rf.becomeFollower(reply.Term)
		rf.mu.Unlock()
		return
	}
	if rf.invalidCandidateState(args.Term) {
		return
	}
	if reply.VoteGranted {
		lablog.Debug(rf.me, lablog.Vote, "vote of term %d granted from peer node: %d", args.Term, peer)
		atomic.AddUint32(grantCount, 1)
	}
	// win condition
	if atomic.LoadUint32(grantCount) >= uint32(rf.majority()) {
		if rf.invalidCandidateState(args.Term) {
			return
		}
		// win election
		rf.sendToChNonBlock(rf.winElectionCh, true)
		return
	}
}

// send to channel in non-blocking operation
func (rf *Raft) sendToChNonBlock(ch chan bool, val bool) {
	select {
	case ch <- val:
	default:
		lablog.Debug(rf.me, lablog.Vote, "chnnel %+v dropped", ch)
	}
}

func (rf *Raft) majority() int {
	return len(rf.peers)/2 + 1
}

func (rf *Raft) invalidCandidateState(term int32) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.getState() != Candidate || rf.getCurrentTerm() != term || rf.killed()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	// Your code here (2B).
	index := -1

	isLeader := rf.getState() == Leader
	if !isLeader || rf.killed() {
		return index, 0, isLeader
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.getCurrentTerm()
	le := LogEntry{
		Term:    term,
		Command: command,
	}
	appendedIndex := rf.appendLog(le)
	lablog.Debug(rf.me, lablog.Start, "start agreement on command %+v at index %d", command, appendedIndex)
	rf.persist()

	// (3A) immediately replicate() for increasing throughput since TestSpeed3A requires 3 ops per heartbeat
	// before: it was too slow on client side since client blocks next operation until it is committed => 1 operation per heartbeat
	rf.replicates()
	return appendedIndex, int(term), isLeader
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		state := rf.getState()
		lablog.Debug(rf.me, lablog.Timer, "ticker: current state: %s, term: %d", state, rf.getCurrentTerm())
		switch state {
		case Leader:
			select {
			case <-rf.stepDownCh:
				// cancel the operation when it becomes follower
			case <-time.After(leaderHeartbeatIntervalMs * time.Millisecond):
				rf.replicates()
			}
		case Follower:
			select {
			case <-rf.heartbeatCh:
				// simply reset timer
			case <-time.After(rf.getElectionTimeout()):
				rf.mu.Lock()
				lablog.Debug(rf.me, lablog.Timer, "election timeout, becoming candidate and start election")
				rf.becomeCandidate()
				rf.mu.Unlock()
			}
		case Candidate:
			// setup election
			rf.mu.Lock()
			term := rf.addCurrentTerm()
			lablog.Debug(rf.me, lablog.Timer, "become candidate at term=%d and start vote", term)
			rf.setVotedFor(int32(rf.me))
			// vote
			voteArg := rf.getReqVoteArg()
			rf.mu.Unlock()
			var grantCount uint32 = 1 // 1 for candidate itself
			for i := range rf.peers {
				if i != rf.me {
					go rf.requestVote(i, &voteArg, &grantCount)
				}
			}

			select {
			case <-time.After(rf.getElectionTimeout()):
				lablog.Debug(rf.me, lablog.Vote, "candidate election canceled at term %d", term)
			case <-rf.stepDownCh:
				// cancel the operation when becomes follower
			case <-rf.winElectionCh:
				if rf.getState() == Candidate {
					rf.mu.Lock()
					rf.becomeLeader()
					rf.mu.Unlock()
				}
			}
		}
	}
}

func (rf *Raft) getReqVoteArg() RequestVoteArgs {
	term, idx := rf.lastLogTermAndIndex()
	voteArg := RequestVoteArgs{
		Term:         rf.getCurrentTerm(),
		CandidateId:  int32(rf.me),
		LastLogTerm:  term,
		LastLogIndex: idx,
	}
	return voteArg
}

func (rf *Raft) getElectionTimeout() time.Duration {
	timeoutMS := labutil.RandRange(electinTimeoutMinMs, electinTimeoutMaxMs)
	return time.Duration(timeoutMS) * time.Millisecond
}

// becomeCandidate changes the state of the Raft server to Candidate.
// only call while holding the lock
func (rf *Raft) becomeCandidate() {
	rf.resetChs()
	rf.setState(Candidate)
}
func (rf *Raft) resetChs() {
	rf.resetCh(rf.winElectionCh)
	rf.resetCh(rf.stepDownCh)
	rf.resetCh(rf.heartbeatCh)
}

// clean up the channel
// cannot simply replace with a new channle, will cause data race.
func (r *Raft) resetCh(ch chan bool) {
L:
	for {
		select {
		case <-ch:
		default:
			break L
		}
	}
}

// becomeLeader changes the state of the Raft server to Leader.
// only call while holding the lock
func (rf *Raft) becomeLeader() {
	lablog.Debug(rf.me, lablog.Leader,
		"win election, becoming leader at term %d",
		rf.getCurrentTerm(),
	)
	rf.resetChs()
	rf.setState(Leader)
	// initialized to leader last log index + 1
	initNextIndex := make([]int, len(rf.peers))
	defaultNextIdx := rf.lastLogIndex() + 1
	for i := range initNextIndex {
		initNextIndex[i] = defaultNextIdx
	}
	rf.setNextIndex(initNextIndex)
	rf.setMatchIndex(make([]int, len(rf.peers)))
}

// appendEntries sends AppendEntries RPCs to all peers
func (rf *Raft) replicates() {
	for nodeID := range rf.peers {
		if nodeID != rf.me {
			go rf.replicaOneNode(nodeID)
		}
	}
}

func (rf *Raft) replicaOneNode(nodeID int) {
	if rf.getState() != Leader {
		return
	}
	// no need to accumulate goroutine since replicates() might got called many times
	if !rf.mu.TryLock() {
		return
	}
	idx := rf.getNodeNextIndex(nodeID)
	currTerm := rf.getCurrentTerm()

	// send append entries or install snapshot
	if idx > rf.baseIndex() {
		// leader still has the log entries to send
		args, err := rf.getAppendEntriesArgs(currTerm, nodeID)
		rf.mu.Unlock()
		if err != nil {
			lablog.Debug(rf.me, lablog.Error, "get append entries args failed: %s", err)
			return
		}
		rf.appendEntries(nodeID, args)
	} else {
		// some follower is too far behind such that the log entries is already compacted to snapshot
		// install snapshot
		args := rf.getInstallSnapshotArgs(currTerm)
		rf.mu.Unlock()
		rf.sendInstallSnapshot(nodeID, args)
	}
}

func (rf *Raft) appendEntries(nodeID int, args *AppendEntriesArgs) {
	rf.mu.Lock() // lock 1
	if rf.getState() != Leader || rf.getCurrentTerm() != args.Term {
		rf.mu.Unlock() // unlock 1
		return
	}
	rf.mu.Unlock()
	lablog.Debug(rf.me, lablog.Append, "append entries to node %d, %+v", nodeID, *args)
	reply, ok := rf.sendAppendEntries(nodeID, args)
	if !ok {
		// even if we don't handle, the next heartbeat will still retry
		lablog.Debug(rf.me, lablog.Error, "send append entries to node %d failed", nodeID)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if reply.Term > args.Term {
		rf.becomeFollower(reply.Term)
		return
	}
	if !reply.Success {
		if reply.Term > args.Term {
			rf.becomeFollower(reply.Term)
			rf.persist()
			return
		}
		lablog.Debug(rf.me, lablog.Append, "term: %d, append entries to node %d failed, retry", args.Term, nodeID)
		if args.PrevLogIndex <= 0 {
			return
		}

		// optimizations for retry logic
		if reply.XTerm != -1 { // follower has the term
			conflictTermIndex := -1
			// #1 Upon receiving a conflict response, the leader should first search its log for conflictTerm.
			// #2 If it finds an entry in its log with that term, it should set nextIndex to be the one beyond the index of the last entry in that term in its log.
			// ref: https://thesquareplanet.com/blog/students-guide-to-raft/#an-aside-on-optimizations
			// #1
			for index := args.PrevLogIndex - 1; index >= 0; index-- {
				if rf.logEntry(index).Term == reply.XTerm {
					conflictTermIndex = index
					break
				}
			}
			if conflictTermIndex != -1 {
				// #2
				lablog.Debug(rf.me, lablog.Append, "try to sync from the term conflict term index=%d", conflictTermIndex)
				if err := rf.setNodeNextIndex(nodeID, conflictTermIndex+1); err != nil { // 2
					lablog.Debug(rf.me, lablog.Error, "set next index failed: %s", err)
					return
				}
			} else {
				// follower does not contains the term
				lablog.Debug(rf.me, lablog.Append, "try to sync with XIndex=%d", reply.XIndex)
				if err := rf.setNodeNextIndex(nodeID, reply.XIndex); err != nil {
					lablog.Debug(rf.me, lablog.Error, "set next index failed: %s", err)
					return
				}
			}
		} else {
			if err := rf.setNodeNextIndex(nodeID, reply.XIndex); err != nil {
				// error might due to it already become follower in another goroutine
				lablog.Debug(rf.me, lablog.Error, "set next index failed: %s", err)
				return
			}
		}
		return
	}
	lablog.Debug(rf.me, lablog.Append, "append entries to node RPC %d success", nodeID)
	// If successful: update nextIndex and matchIndex for follower
	matchedIndex := args.PrevLogIndex + len(args.Logs)
	if err := rf.setNodeMatchIndex(nodeID, matchedIndex); err != nil {
		lablog.Debug(rf.me, lablog.Error, "set match index failed: %s", err)
		return
	}
	if err := rf.setNodeNextIndex(nodeID, matchedIndex+1); err != nil {
		lablog.Debug(rf.me, lablog.Error, "set next index failed: %s", err)
		return
	}
	rf.commit(nodeID, args.Term)
	lablog.Debug(rf.me, lablog.Append, "append entries to node %d success, matchIndex=%d, nextIndex=%d", nodeID, matchedIndex, rf.getNodeNextIndex(nodeID))
}

// commit update the commitIndex state after checking the majority of matchIndex
func (rf *Raft) commit(nodeID int, term int32) {
	// If there exists an N such that N > commitIndex, a majority
	// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N (§5.3, §5.4).
	N := rf.matchIndex[nodeID]
	lablog.Debug(rf.me, lablog.Commit, "checking commit node %d at term %d: N=%d", nodeID, term, N)
	count := 1 // 1 for leader itself
	if N > rf.commitIndex {
		for i := range rf.peers {
			if rf.matchIndex[i] >= N {
				count++
			}
		}
		// Why rf.Log[N].Term == term ?
		// To eliminate problems like the one in Figure 8,
		// Raft never commits log entries from previous terms by counting replicas.
		// Only log entries from the leader’s current term are committed by counting replicas; ($5.4.2)
		if count >= rf.majority() && rf.logEntry(N).Term == term {
			lablog.Debug(rf.me, lablog.Commit, "commit index to index %d at term %d", N, term)
			rf.setCommitIndex(N)
			rf.applyCond.Signal()
			return
		}
	}
}

// If commitIndex > lastApplied: increment lastApplied,
// apply log[lastApplied] to state machine (§5.3)
func (rf *Raft) applyMsgs() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.getLastApplied() >= rf.commitIndex {
			rf.applyCond.Wait() // will release lock
		}
		lastApplied, commitIdx := rf.getLastApplied(), rf.commitIndex
		msgs := make([]ApplyMsg, 0, commitIdx-lastApplied)
		for _, entry := range rf.logsRange(lastApplied+1, commitIdx) {
			msg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
			msgs = append(msgs, msg)
		}
		rf.mu.Unlock()

		// don't hold lock while sending to applyCh
		// since applyCh make client app to call Snapshot which will lead to lock contention
		for _, msg := range msgs {
			rf.applyCh <- msg
		}
		rf.mu.Lock()
		rf.setLastApplied(commitIdx)
		lablog.Debug(rf.me, lablog.Info, "applied %d messages, set last applied: %d", len(msgs), commitIdx)
		rf.mu.Unlock()
	}
}

// getAppendEntriesArgs returns the AppendEntriesArgs for the given term and nodeId
func (rf *Raft) getAppendEntriesArgs(term int32, nodeID int) (*AppendEntriesArgs, error) {
	args := &AppendEntriesArgs{
		Term:         term,
		LeaderId:     int32(rf.me),
		PrevLogIndex: 0,
		Logs:         nil,
		LeaderCommit: rf.commitIndex,
	}
	// If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
	entries, prevEntry, err := rf.getNodeEntries(nodeID)
	if err != nil {
		return nil, fmt.Errorf("get node entries failed: %s", err)
	}
	if prevEntry != nil {
		args.PrevLogTerm = prevEntry.Term
		args.PrevLogIndex = prevEntry.Index
	}
	if len(entries) > 0 {
		args.Logs = make([]LogEntry, len(entries))
		copy(args.Logs, entries)
	} // else is heartbeat
	return args, nil
}

func (rf *Raft) sendAppendEntries(nodeId int, args *AppendEntriesArgs) (*AppendEntriesReply, bool) {
	lablog.Debug(rf.me, lablog.Heart, "send append entries to node %d", nodeId)
	reply := &AppendEntriesReply{}
	ok := rf.peers[nodeId].Call("Raft.AppendEntries", args, reply)
	return reply, ok
}

func (rf *Raft) getInstallSnapshotArgs(term int32) *InstallSnapshotArgs {
	base := rf.logEntry(rf.baseIndex())
	args := &InstallSnapshotArgs{
		Term:              term,
		LeaderId:          int32(rf.me),
		LastIncludedIndex: base.Index,
		LastIncludedTerm:  base.Term,
		Snapshot:          rf.persister.ReadSnapshot(),
	}
	return args
}

// This happens when the leader has already discarded the next log entry that it needs to send to a follower.
func (rf *Raft) sendInstallSnapshot(nodeID int, args *InstallSnapshotArgs) {
	reply := &InstallSnapshotReply{}
	ok := rf.peers[nodeID].Call("Raft.InstallSnapshot", args, reply)
	if !ok {
		// even if we don't handle, the next heartbeat will still retry
		lablog.Debug(rf.me, lablog.Error, "send install snapshot to node %d failed", nodeID)
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if reply.Term > args.Term {
		rf.becomeFollower(reply.Term)
		return
	}
	// the follower already synced with leader's snapshot
	if err := rf.setNodeMatchIndex(nodeID, args.LastIncludedIndex); err != nil {
		lablog.Debug(rf.me, lablog.Error, "set match index failed: %s", err)
		return
	}
	if err := rf.setNodeNextIndex(nodeID, args.LastIncludedIndex+1); err != nil {
		lablog.Debug(rf.me, lablog.Error, "set next index failed: %s", err)
		return
	}
}

type AppendEntriesArgs struct {
	Term     int32
	LeaderId int32
	//index of log entry immediately preceding new ones, rf.getNextIndex(nodeId)-1
	PrevLogIndex int
	PrevLogTerm  int32
	Logs         []LogEntry
	// leader’s commitIndex
	LeaderCommit int
}

type AppendEntriesReply struct {
	Success bool
	Term    int32
	XTerm   int32 //  term in the conflicting entry (if any)
	XIndex  int   // the first index it stores for the conflicting term
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm := rf.getCurrentTerm()
	reply.Success = false
	reply.Term = currentTerm
	isHeartbeat := len(args.Logs) == 0
	if isHeartbeat {
		lablog.Debug(rf.me, lablog.Heart,
			"received heartbeat from leader %d. %+v at currentTerm=%d",
			args.LeaderId,
			*args,
			currentTerm,
		)
	} else {
		lablog.Debug(
			rf.me,
			lablog.Info,
			"received AppendEntries from leader %d. %+v at currentTerm=%d",
			args.LeaderId,
			*args,
			currentTerm,
		)
	}
	// Reply false if term < currentTerm (§5.1)
	if args.Term < currentTerm {
		return
	}
	// Current terms are exchanged whenever servers communicate;
	// if one server’s currentterm is smaller than the other’s, then it updates its current term to the larger value.
	rf.setCurrentTerm(args.Term)
	// to reset election timeout
	if rf.getState() == Follower {
		rf.heartbeatCh <- true
	}
	// If a candidate or leader discovers that its term is out of date, it immediately reverts to follower state.
	if args.Term > currentTerm || rf.getState() != Follower {
		rf.becomeFollower(args.Term)
	}

	// reply
	if isHeartbeat {
		reply.Success = true
	} else {
		reply.Success = false
	}
	reply.XTerm, reply.XIndex = -1, -1
	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex > rf.lastLogIndex() {
		// If a follower does not have prevLogIndex in its log,
		// it should return with conflictIndex = len(log) and conflictTerm = None.
		// ref: https://thesquareplanet.com/blog/students-guide-to-raft/#an-aside-on-optimizations
		lablog.Debug(rf.me, lablog.Append, "not success: log too short, prevLogIndex=%d, but log last index=%d", args.PrevLogIndex, rf.lastLogIndex())
		reply.Success = false
		reply.XIndex = rf.lastLogIndex() + 1
		rf.persist()
		return
	} else if args.PrevLogIndex >= 0 && rf.logEntry(args.PrevLogIndex).Term != args.PrevLogTerm {
		lablog.Debug(rf.me, lablog.Append, "not success 2 prevLogIndex=%d, prevLogTerm=%d, but log[%d].Term=%d", args.PrevLogIndex, args.PrevLogTerm, args.PrevLogIndex, rf.logEntry(args.PrevLogIndex).Term)
		// If an existing entry conflicts with a new one (same index but different terms)
		reply.Success = false
		// If a follower does have prevLogIndex in its log, but the term does not match,
		// it should return conflictTerm = log[prevLogIndex].Term, and then search its log for the first index whose entry has term equal to conflictTerm.
		// ref: https://thesquareplanet.com/blog/students-guide-to-raft/#an-aside-on-optimizations
		conflictingTerm := rf.logEntry(args.PrevLogIndex).Term
		reply.XTerm = conflictingTerm
		for i := 1; i <= args.PrevLogIndex; i++ {
			if rf.logEntry(i).Term == conflictingTerm {
				reply.XIndex = i
				break
			}
		}
		rf.persist()
		return
	} else {
		lablog.Debug(rf.me, lablog.Append, "append logs %+v", args.Logs)
		rf.appendLogs(args.PrevLogIndex, args.Logs) // delete the existing entry and all that follow it and Append any new entries not already in the log
		reply.Success = true
	}
	// only when success
	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.setCommitIndex(labutil.Min(args.LeaderCommit, rf.lastLogIndex()))
		rf.applyCond.Signal()
	}
	rf.persist()
}

type InstallSnapshotArgs struct {
	Term     int32
	LeaderId int32
	// the last included entry index in the snapshot
	LastIncludedIndex int
	// the last included entry term in the snapshot
	LastIncludedTerm int32
	Snapshot         []byte
}

type InstallSnapshotReply struct {
	Term int32
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	lablog.Debug(rf.me, lablog.Snapshot, "received snapshot from leader %d at term %d. %+v ", args.LeaderId, rf.getCurrentTerm(), *args)
	if args.Term < rf.getCurrentTerm() {
		rf.mu.Unlock()
		return
	}
	if args.Term > rf.getCurrentTerm() && rf.getState() != Follower {
		rf.becomeFollower(args.Term)
		rf.persist()
	}

	reply.Term = rf.getCurrentTerm()
	rf.mu.Unlock()
	// IMPORTANT: to reset election timeout
	// otherwise, the follower will frequently timeout and start a new election
	rf.heartbeatCh <- true

	// asychronously apply the snapshot
	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Snapshot,
			SnapshotTerm:  int(args.LastIncludedTerm),
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()

}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:         peers,
		persister:     persister,
		me:            me,
		raftState:     newRaftState(),
		stepDownCh:    make(chan bool),
		winElectionCh: make(chan bool),
		heartbeatCh:   make(chan bool),
		applyCh:       applyCh,
	}
	rf.applyCond = sync.NewCond(&rf.mu)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyMsgs() // start applyMsgs goroutine
	return rf
}
