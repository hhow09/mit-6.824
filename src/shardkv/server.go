package shardkv

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/lablog"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

const (
	timeout              = 3 * time.Second
	updateConfigInterval = 100 * time.Millisecond
)

type ShardKV struct {
	mu           sync.RWMutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead    int32 // set by Kill()
	resChan map[int]chan reply
	// lastOperation tracks the last operation for each client for de-dup
	// since unreliable network may cause duplicated requests.
	// It's maintained as a map from client id to last operation
	lastOperation map[int64]ClientOpRecord
	// above are same as lab 3
	ctrlerClient  *shardctrler.Clerk
	currentConfig shardctrler.Config
	lastConfig    shardctrler.Config
	shards        Shards
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	rep := kv.handleOp(Op{
		Type:      OpGet,
		Key:       args.Key,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	})
	reply.Value = rep.Value
	reply.Err = rep.Err
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
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
	reply.Err = rep.Err
}

func (kv *ShardKV) handleOp(op Op) reply {
	cmd := NewOpCommand(op)
	idx, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		return reply{Err: ErrWrongLeader}
	}
	kv.mu.Lock()
	resChan, remove := kv.addResChan(idx)
	kv.mu.Unlock()
	defer remove()
	select {
	case res := <-resChan:
		return res
	case <-time.After(timeout):
		return reply{Err: ErrTimeout}
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(Command{})
	labgob.Register(shardctrler.Config{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.resChan = make(map[int]chan reply)
	kv.shards = NewShards(shardctrler.NShards)
	kv.lastOperation = make(map[int64]ClientOpRecord)
	kv.ctrlerClient = shardctrler.MakeClerk(ctrlers)
	kv.currentConfig = kv.ctrlerClient.Query(-1)

	// snapshot
	b := persister.ReadSnapshot()
	if len(b) > 0 {
		if err := kv.restoreSnapshot(b); err != nil {
			lablog.Debug(kv.me, lablog.Snapshot, "KVServer failed to restore snapshot: %w", err)
		}
	}

	// goroutines
	go kv.apply()
	go kv.monitorConfigChange()

	return kv
}

func (kv *ShardKV) apply() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			kv.mu.Lock()
			var rep reply
			cmd := msg.Command.(Command)
			switch cmd.Type {
			case CommandOp:
				op := cmd.Data.(Op)
				rep = kv.applyOp(op)

			case CommandConfig:
				nxtConfig := cmd.Data.(shardctrler.Config)
				rep = kv.applyConfig(&nxtConfig)

			}
			// only reply to CommandOp
			// only leader can reply
			// since here could get the message from previous term (started by previous leader), we need to check the term
			if currTerm, isLeader := kv.rf.GetState(); cmd.Type == CommandOp && isLeader && currTerm == int(msg.CommandTerm) {
				ch := kv.getResChan(msg.CommandIndex)
				kv.mu.Unlock()
				ch <- rep
			} else {
				kv.mu.Unlock()
			}

			// Whenever your key/value server detects that the Raft state size is approaching this threshold,
			// it should save a snapshot using Snapshot, which in turn uses persister.SaveRaftState().
			if kv.needSnapshot() {
				if err := kv.snapshot(msg.CommandIndex); err != nil {
					lablog.Debug(kv.me, lablog.Snapshot, "KVServer failed to snapshot: %w", err)
				}
			}
		} else if msg.SnapshotValid {
			kv.mu.Lock()
			if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
				if err := kv.restoreSnapshot(msg.Snapshot); err != nil {
					lablog.Debug(kv.me, lablog.Snapshot, "KVServer failed to restore snapshot: %w", err)
				}
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) applyOp(op Op) reply {
	shardID := key2shard(op.Key)
	if !kv.canServeThisShard(shardID) {
		return reply{Err: ErrWrongGroup}
	}
	// put and append: de duplicate write operation
	// get: read-only, we could read state machine directly to get latest value
	// if we de-dup get, result in non-linearizable bug.
	if record, ok := kv.alreadyRepliedRecord(op); op.Type != OpGet && ok {
		return record.Reply
		// skip applying to state machine
	} else {
		rep := kv.applyToShards(op, shardID)
		if op.Type != OpGet {
			kv.setLastOperation(op, rep)
		}
		return rep
	}
}

func (kv *ShardKV) applyConfig(nextConfig *shardctrler.Config) reply {
	if nextConfig.Num != kv.currentConfig.Num+1 {
		return reply{Value: "", Err: ErrStaleConfig}
	}
	// TODO update shards
	kv.lastConfig = kv.currentConfig
	kv.currentConfig = *nextConfig
	return reply{OK, ""}
}

// whether the server can serve this shard
func (kv *ShardKV) canServeThisShard(shardID int) bool {
	// TODO check shard status
	return kv.currentConfig.Shards[shardID] == kv.gid
}

func (kv *ShardKV) applyToShards(op Op, shardID int) reply {
	rep := reply{}
	shard := kv.shards[shardID]
	switch op.Type {
	case OpGet:
		value := shard.Get(op.Key)
		rep = reply{Value: value}
	case OpPut:
		shard.Put(op.Key, op.Value)
	case OpAppend:
		shard.Append(op.Key, op.Value)
	}
	return rep
}

// monitorConfigChange checks if there is a new configuration
// Your server will need to periodically poll the shardctrler to learn about new configurations.
// The tests expect that your code polls roughly every 100 milliseconds;
// more often is OK, but much less often may cause problems.
func (kv *ShardKV) monitorConfigChange() {
	for !kv.killed() {
		kv.mu.RLock()
		currConfigNum := kv.currentConfig.Num
		kv.mu.RUnlock()
		if _, isLeader := kv.rf.GetState(); isLeader {
			nextConfig := kv.ctrlerClient.Query(currConfigNum + 1)
			fmt.Printf("kv %d, nextConfig: %+v\n", kv.me, nextConfig)
			// TODO migrate existing shards
			if nextConfig.Num == currConfigNum+1 {
				kv.rf.Start(NewConfigCommand(nextConfig))
			}
		}
		time.Sleep(updateConfigInterval)
	}
}

func (kv *ShardKV) snapshot(idx int) error {
	// TODO
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := kv.shards.EncodeSnapshot(e); err != nil {
		return err
	}
	if err := e.Encode(kv.lastOperation); err != nil {
		return err
	}
	kv.rf.Snapshot(idx, w.Bytes())
	return nil
}

func (kv *ShardKV) restoreSnapshot(snapshot []byte) error {
	// TODO
	if len(snapshot) == 0 {
		return errors.New("empty snapshot")
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if err := kv.shards.DecodeSnapshot(d); err != nil {
		return fmt.Errorf("failed to decode state machine: %w", err)
	}
	if err := d.Decode(&kv.lastOperation); err != nil {
		return fmt.Errorf("failed to decode last operation: %w", err)
	}
	return nil
}

// server state

func (kv *ShardKV) addResChan(idx int) (chan reply, func()) {
	kv.resChan[idx] = make(chan reply, 1)
	remove := func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		close(kv.resChan[idx])
		delete(kv.resChan, idx)
	}
	return kv.resChan[idx], remove
}

func (kv *ShardKV) getResChan(idx int) chan reply {
	return kv.resChan[idx]
}

// alreadyRepliedRecord checks if the server has already replied to the client
func (kv *ShardKV) alreadyRepliedRecord(op Op) (ClientOpRecord, bool) {
	if record, ok := kv.lastOperation[op.ClientID]; ok && record.RequestID >= op.RequestID {
		return record, true
	}
	return ClientOpRecord{}, false
}

// setLastOperation sets the last operation of a client
func (kv *ShardKV) setLastOperation(op Op, rep reply) {
	kv.lastOperation[op.ClientID] = ClientOpRecord{
		RequestID: op.RequestID,
		Reply:     rep,
	}
}

func (kv *ShardKV) needSnapshot() bool {
	// You should compare maxraftstate to persister.RaftStateSize()
	// If maxraftstate is -1, you do not have to snapshot.
	if kv.maxraftstate != -1 && kv.rf.RaftStateSize() >= kv.maxraftstate {
		return true
	}
	return false
}
