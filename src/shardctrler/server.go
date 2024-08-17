package shardctrler

import (
	"fmt"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	stateMachine *InMemoryStateMachine
	configs      []Config // indexed by config num
	resChan      map[int]chan reply
	// lastOperation tracks the last operation for each client for de-dup
	// since unreliable network may cause duplicated requests.
	// It's maintained as a map from client id to last operation
	lastOperation map[int64]ClientOpRecord
}

const (
	timeout = 3 * time.Second
)

type OpType string

func (opType OpType) String() string {
	return string(opType)
}

var _ fmt.Stringer = (*OpType)(nil)

const (
	OpJoin  OpType = "Join"
	OpLeave OpType = "Leave"
	OpMove  OpType = "Move"
	OpQuery OpType = "Get" // read only
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type    OpType
	Servers map[int][]string // join
	GIDs    []int            // leave
	GID     int              // move
	Shard   int              // move
	Num     int              // query
	// (ClientID, RequestID) uniquely identify a client request
	// they are used for de-dup check in apply()
	ClientID  int64
	RequestID int64
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{
		Type:      OpJoin,
		Servers:   args.Servers,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}
	res := sc.handleOp(op)
	reply.WrongLeader = res.WrongLeader
	reply.Err = res.Err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{
		Type:      OpLeave,
		GIDs:      args.GIDs,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}
	res := sc.handleOp(op)
	reply.WrongLeader = res.WrongLeader
	reply.Err = res.Err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		Type:      OpMove,
		Shard:     args.Shard,
		GID:       args.GID,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}
	res := sc.handleOp(op)
	reply.WrongLeader = res.WrongLeader
	reply.Err = res.Err

}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{
		Type:      OpQuery,
		Num:       args.Num,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}
	res := sc.handleOp(op)
	reply.Config = res.Config
	reply.WrongLeader = res.WrongLeader
}

func (sc *ShardCtrler) handleOp(op Op) reply {
	idx, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		return reply{WrongLeader: true}
	}
	sc.mu.Lock()
	resChan, remove := sc.addResChan(idx)
	sc.mu.Unlock()
	defer remove()
	select {
	case res := <-resChan:
		return res
	case <-time.After(timeout):
		return reply{Err: ErrTimeout}
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.resChan = make(map[int]chan reply)
	sc.stateMachine = NewInMemoryStateMachine()
	sc.lastOperation = make(map[int64]ClientOpRecord)

	go sc.apply()
	return sc
}

func (sc *ShardCtrler) apply() {
	for {
		msg := <-sc.applyCh
		if msg.CommandValid {
			// de-dup
			sc.mu.Lock()
			var rep reply
			op := msg.Command.(Op)
			// put and append: de duplicate write operation
			// get: read-only, we could read state machine directly to get latest value
			// if we de-dup get, result in non-linearizable bug.
			if record, ok := sc.alreadyRepliedRecord(op); op.Type != OpQuery && ok {
				rep = record.Reply
				// skip applying to state machine
			} else {
				rep = sc.applyToStateMachine(msg)
				if op.Type != OpQuery {
					sc.setLastOperation(op, rep)
				}
			}
			// only leader can reply
			// since here could get the message from previous term (started by previous leader), we need to check the term
			if currTerm, isLeader := sc.rf.GetState(); isLeader && currTerm == int(msg.CommandTerm) {
				ch := sc.getResChan(msg.CommandIndex)
				sc.mu.Unlock()
				ch <- rep
			} else {
				sc.mu.Unlock()
			}
		}
	}
}

// server state

func (sc *ShardCtrler) addResChan(idx int) (chan reply, func()) {
	sc.resChan[idx] = make(chan reply, 1)
	remove := func() {
		sc.mu.Lock()
		defer sc.mu.Unlock()
		close(sc.resChan[idx])
		delete(sc.resChan, idx)
	}
	return sc.resChan[idx], remove
}

func (sc *ShardCtrler) getResChan(idx int) chan reply {
	return sc.resChan[idx]
}

// alreadyRepliedRecord checks if the server has already replied to the client
func (sc *ShardCtrler) alreadyRepliedRecord(op Op) (ClientOpRecord, bool) {
	if record, ok := sc.lastOperation[op.ClientID]; ok && record.RequestID >= op.RequestID {
		return record, true
	}
	return ClientOpRecord{}, false
}

// setLastOperation sets the last operation of a client
func (sc *ShardCtrler) setLastOperation(op Op, rep reply) {
	sc.lastOperation[op.ClientID] = ClientOpRecord{
		RequestID: op.RequestID,
		Reply:     rep,
	}
}

func (sc *ShardCtrler) applyToStateMachine(msg raft.ApplyMsg) reply {
	op := msg.Command.(Op)
	rep := reply{}
	switch op.Type {
	case OpJoin:
		sc.stateMachine.Join(op.Servers)
	case OpLeave:
		sc.stateMachine.Leave(op.GIDs)
	case OpMove:
		sc.stateMachine.Move(op.GID, op.Shard)
	case OpQuery:
		rep.Config = sc.stateMachine.Query(op.Num)
	}
	return rep
}
