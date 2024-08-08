package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const (
	timeout = 3 * time.Second
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type OpType uint8

const (
	OpGet OpType = iota
	OpPut
	OpAppend
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  OpType
	Key   string
	Value string
	// (ClientID, RequestID) uniquely identify a client request
	// they are used for de-dup check in apply()
	ClientID  int64
	RequestID int64
}

type stateMachine interface {
	Get(key string) string
	Put(key string, value string)
	Append(key string, value string)
}

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	currTerm     uint32
	resChan      map[int]chan reply
	stateMachine stateMachine
	// lastOperation tracks the last operation for each client for de-dup
	// since unreliable network may cause duplicated requests.
	// It's maintained as a map from client id to last operation
	lastOperation map[int64]ClientOpRecord
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	rep := kv.handleOp(Op{
		Type:      OpGet,
		Key:       args.Key,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	})
	reply.Value = rep.value
	reply.Err = rep.err
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Key:       args.Key,
		Value:     args.Value,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}
	switch args.Op {
	case "Put":
		op.Type = OpPut
	case "Append":
		op.Type = OpAppend
	}
	rep := kv.handleOp(op)
	reply.Err = rep.err
}

func (kv *KVServer) handleOp(op Op) reply {
	idx, currTerm, isLeader := kv.rf.Start(op)
	if !isLeader {
		return reply{err: ErrNotLeader}
	}
	kv.mu.Lock()
	kv.setCurrTerm(currTerm)
	resChan, remove := kv.addResChan(idx)
	kv.mu.Unlock()
	defer remove()
	select {
	case res := <-resChan:
		return res
	case <-time.After(timeout):
		return reply{err: ErrTimeout}
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.resChan = make(map[int]chan reply)

	// You may need initialization code here.
	kv.stateMachine = NewInMemoryStateMachine()
	kv.lastOperation = make(map[int64]ClientOpRecord)
	go kv.apply()
	return kv
}

func (kv *KVServer) apply() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			// de-dup
			kv.mu.Lock()
			var rep reply
			op := msg.Command.(Op)
			if record, ok := kv.alreadyRepliedRecord(op); ok {
				rep = record.reply
				// skip applying to state machine
			} else {
				rep = kv.applyToStateMachine(msg)
				if op.Type != OpGet {
					kv.setLastOperation(op, rep)
				}
			}
			// only leader can reply
			// since here could get the message from previous term (started by previous leader), we need to check the term
			if currTerm, isLeader := kv.rf.GetState(); isLeader && currTerm == int(msg.CommandTerm) {
				ch := kv.getResChan(msg.CommandIndex)
				kv.mu.Unlock()
				ch <- rep
			} else {
				kv.mu.Unlock()
			}
		}
	}
}

func (kv *KVServer) applyToStateMachine(msg raft.ApplyMsg) reply {
	op := msg.Command.(Op)
	rep := reply{}
	switch op.Type {
	case OpGet:
		value := kv.stateMachine.Get(op.Key)
		rep = reply{value: value}
	case OpPut:
		kv.stateMachine.Put(op.Key, op.Value)
	case OpAppend:
		kv.stateMachine.Append(op.Key, op.Value)
	}
	return rep
}

// server state

// already known 0 <= term < 2^32
func (kv *KVServer) setCurrTerm(term int) {
	atomic.StoreUint32(&kv.currTerm, uint32(term))
}

func (kv *KVServer) getCurrTerm() int {
	return int(atomic.LoadUint32(&kv.currTerm))
}

func (kv *KVServer) addResChan(idx int) (chan reply, func()) {
	kv.resChan[idx] = make(chan reply, 1)
	remove := func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		close(kv.resChan[idx])
		delete(kv.resChan, idx)
	}
	return kv.resChan[idx], remove
}

func (kv *KVServer) getResChan(idx int) chan reply {
	return kv.resChan[idx]
}

// alreadyRepliedRecord checks if the server has already replied to the client
func (kv *KVServer) alreadyRepliedRecord(op Op) (ClientOpRecord, bool) {
	if record, ok := kv.lastOperation[op.ClientID]; ok && record.RequestID >= op.RequestID {
		return record, true
	}
	return ClientOpRecord{}, false
}

// setLastOperation sets the last operation of a client
func (kv *KVServer) setLastOperation(op Op, rep reply) {
	kv.lastOperation[op.ClientID] = ClientOpRecord{
		RequestID: op.RequestID,
		reply:     rep,
	}
}
