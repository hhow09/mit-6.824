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
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/lablog"
	"6.824/labrpc"
	"6.824/labutil"
)

// 6.824: The tester requires that the leader send heartbeat RPCs no more than ten times per second.
const (
	leaderHeartbeatIntervalMs = 100
	// should keep enough range to avoid split vote
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
	Index int
	Term  int
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
	Log []LogEntry

	// channels for state machine
	stepDownCh    chan bool
	winElectionCh chan bool
	heartbeatCh   chan bool
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
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int32
	CandidateId  int32
	LastLogIndex int
	LastLogTerm  int
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
	if canVoteForThisCandidate {
		lablog.Debug(rf.me, lablog.Vote, "grant vote to candidate=%d at Term %d", args.CandidateId, args.Term)
		reply.VoteGranted = true
		rf.setVotedFor(args.CandidateId)
	}
}

// becomeFollower changes the state of the Raft server to Follower.
// only call while holding the lock
func (rf *Raft) becomeFollower(term int32) {
	lablog.Debug(rf.me, lablog.Info, "become follower at term=%d", term)
	rf.setState(Follower)
	rf.setCurrentTerm(term)
	rf.setVotedFor(voteForNull)
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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
		rf.mu.Lock()
		state := rf.getState()
		lablog.Debug(rf.me, lablog.Timer, "ticker: current state: %s, term: %d", state, rf.getCurrentTerm())
		rf.mu.Unlock()
		switch state {
		case Leader:
			select {
			case <-rf.stepDownCh:
				// cancel the operation when it becomes follower
			case <-time.After(leaderHeartbeatIntervalMs * time.Millisecond):
				rf.mu.Lock()
				// rf.appendEntries()
				rf.mu.Unlock()
			}
		case Follower:
			select {
			case <-rf.heartbeatCh:
				// simply reset timer
			case <-time.After(rf.getElectionTimeout()):
				rf.mu.Lock()
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
				rf.mu.Lock()
				if rf.getState() == Candidate {
					rf.becomeLeader()
				}
				rf.mu.Unlock()
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
func (r *Raft) resetChs() {
	r.winElectionCh = make(chan bool)
	r.stepDownCh = make(chan bool)
	r.heartbeatCh = make(chan bool)
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
