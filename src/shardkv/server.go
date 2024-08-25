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
	timeout         = 3 * time.Second
	monitorInterval = 100 * time.Millisecond
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

// =================
// CLIENT-FACING RPC
// =================

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
	labgob.Register(ShardInterServerRequest{})
	labgob.Register(ShardInterServerResponse{})

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
	go kv.monitorPulling()
	// TODO

	return kv
}

func (kv *ShardKV) apply() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			kv.mu.Lock()
			var rep reply
			cmd := msg.Command.(Command)
			lablog.DebugS(kv.gid, kv.me, lablog.Apply, "KVServer %d apply command %+v", kv.me, cmd)
			switch cmd.Type {
			case CommandOp:
				op := cmd.Data.(Op)
				rep = kv.applyOp(op)

			case CommandConfig:
				nxtConfig := cmd.Data.(shardctrler.Config)
				rep = kv.applyConfig(&nxtConfig)
			case CommandInsertShard:
				res := cmd.Data.(ShardInterServerResponse)
				kv.applyInsertShards(&res)
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
	lablog.DebugS(kv.gid, kv.me, lablog.SConfig, "applyConfig %+v", nextConfig)
	kv.updateShardStatus(nextConfig)
	kv.lastConfig = kv.currentConfig
	kv.currentConfig = *nextConfig
	return reply{"OK", ""} // actually not read by client
}

// updateShardStatus mark the shards that need to be migrated
func (kv *ShardKV) updateShardStatus(nextConfig *shardctrler.Config) {
	for i := 0; i < shardctrler.NShards; i++ {
		// this shard will join to this group
		if kv.currentConfig.Shards[i] != kv.gid && nextConfig.Shards[i] == kv.gid {
			gid := kv.currentConfig.Shards[i]
			if gid != 0 {
				lablog.DebugS(kv.gid, kv.me, lablog.SConfig, "shard %d will be pulled from %d", i, gid)
				kv.shards[i].SetStatus(Pulling)
			}
		}
		// this shard doesn't belong to this group
		if kv.currentConfig.Shards[i] == kv.gid && nextConfig.Shards[i] != kv.gid {
			gid := nextConfig.Shards[i]
			if gid != 0 {
				lablog.DebugS(kv.gid, kv.me, lablog.SConfig, "shard %d will be pulled by %d", i, gid)
				kv.shards[i].SetStatus(PulledByOthers)
			}
		}
	}
}

// whether the server can serve this shard
func (kv *ShardKV) canServeThisShard(shardID int) bool {
	shard := kv.shards[shardID]
	return kv.currentConfig.Shards[shardID] == kv.gid && (shard.GetStatus() == Serving || shard.GetStatus() == Cleaning)
}

func (kv *ShardKV) applyToShards(op Op, shardID int) reply {
	lablog.DebugS(kv.gid, kv.me, lablog.Apply, "KVServer %d applyToShards %+v", kv.me, op)
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

// applyInsertShards insert the shards to the state machine
func (kv *ShardKV) applyInsertShards(response *ShardInterServerResponse) {
	if response.ConfigNum != kv.currentConfig.Num {
		lablog.DebugS(kv.gid, kv.me, lablog.Apply, "KVServer %d reject stale response from stal config %+v", kv.me, response)
		return
	}

	lablog.DebugS(kv.gid, kv.me, lablog.Apply, "KVServer %d applyInsertShards %+v", kv.me, response)
	for shardId, shardData := range response.Shards {
		shard := kv.shards[shardId]
		if shard.GetStatus() == Pulling {
			kv.shards[shardId] = NewNewShardFromData(shardData, Cleaning)
		} else {
			// duplicated insert
			break
		}
	}
	for clientId, rec := range response.LastOperation {
		if _, ok := kv.lastOperation[clientId]; !ok {
			kv.lastOperation[clientId] = rec
		}
	}
}

// ===============
// MONITOR ROUTINE
// ===============

// monitorConfigChange checks if there is a new configuration
// Your server will need to periodically poll the shardctrler to learn about new configurations.
// The tests expect that your code polls roughly every 100 milliseconds;
// more often is OK, but much less often may cause problems.
func (kv *ShardKV) monitorConfigChange() {
	for !kv.killed() {
		kv.mu.RLock()
		currConfigNum := kv.currentConfig.Num
		// the wait for current shard migration to finish
		if !kv.shards.ReadyForNewConfig() {
			kv.mu.RUnlock()
			time.Sleep(monitorInterval)
			continue
		}

		kv.mu.RUnlock()
		if _, isLeader := kv.rf.GetState(); isLeader {
			nextConfig := kv.ctrlerClient.Query(currConfigNum + 1)
			if nextConfig.Num == currConfigNum+1 {
				kv.rf.Start(NewConfigCommand(nextConfig))
			}
		}
		time.Sleep(monitorInterval)
	}
}

func (kv *ShardKV) monitorPulling() {
	for !kv.killed() {
		kv.mu.RLock()
		groups := kv.getGIDShardIDsByStatus(Pulling)
		var wg sync.WaitGroup
		cfgNum := kv.currentConfig.Num
		for _, gp := range groups {
			wg.Add(1)
			go func(servers []string, shrardIDs []int, cfgNum int) {
				req := ShardInterServerRequest{
					ConfigNum: cfgNum,
					ShardIDs:  shrardIDs,
				}
				for _, server := range servers {
					srv := kv.make_end(server)
					var resp ShardInterServerResponse
					if srv.Call("ShardKV.PullShardRPC", &req, &resp) && resp.Err == "" {
						kv.rf.Start(NewInsertShardCommand(resp))
					}
				}

			}(gp.servers, gp.shardIDs, cfgNum)
		}
		kv.mu.RUnlock()
		wg.Wait()
		time.Sleep(monitorInterval)
	}
}

type groupResponse struct {
	servers  []string
	shardIDs []int
}

// getGIDShardIDsByStatus returns a list of filtered groupServerAndShards
func (kv *ShardKV) getGIDShardIDsByStatus(s ShardStatus) []groupResponse {
	gid2shardIDs := make(map[int][]int)
	ids := kv.shards.ShardsByStatus(s)
	for _, i := range ids {
		// use lastConfig since we are migrating the last config
		gid := kv.lastConfig.Shards[i]
		if gid != 0 {
			if _, ok := gid2shardIDs[gid]; !ok {
				gid2shardIDs[gid] = make([]int, 0)
			}
			gid2shardIDs[gid] = append(gid2shardIDs[gid], i)
		}
	}
	res := make([]groupResponse, 0)
	for gid, shardIDs := range gid2shardIDs {
		res = append(res, groupResponse{
			servers:  kv.lastConfig.Groups[gid],
			shardIDs: shardIDs,
		})
	}
	return res
}

func (kv *ShardKV) snapshot(idx int) error {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := kv.shards.EncodeSnapshot(e); err != nil {
		return err
	}
	if err := e.Encode(kv.lastOperation); err != nil {
		return err
	}
	if err := e.Encode(kv.currentConfig); err != nil {
		return err
	}
	if err := e.Encode(kv.lastConfig); err != nil {
		return err
	}
	kv.rf.Snapshot(idx, w.Bytes())
	return nil
}

func (kv *ShardKV) restoreSnapshot(snapshot []byte) error {
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
	if err := d.Decode(&kv.currentConfig); err != nil {
		return fmt.Errorf("failed to decode current config: %w", err)
	}
	if err := d.Decode(&kv.lastConfig); err != nil {
		return fmt.Errorf("failed to decode last config: %w", err)
	}
	return nil
}

// ================
// INTER-SERVER RPC
// ================
func (kv *ShardKV) PullShardRPC(request *ShardInterServerRequest, response *ShardInterServerResponse) {
	lablog.DebugS(kv.gid, kv.me, lablog.ShardOp, "KVServer %d received PullShardRPC %+v", kv.me, request)
	// only pull shards from leader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		response.Err = ErrWrongLeader
		return
	}
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	if kv.currentConfig.Num < request.ConfigNum {
		response.Err = ErrNotReady
		return
	}

	// If one of your RPC handlers includes in its reply a map (e.g. a key/value map) that's part of your server's state, you may get bugs due to races.
	// The RPC system has to read the map in order to send it to the caller, but it isn't holding a lock that covers the map. Your server, however, may proceed to modify the same map while the RPC system is reading it.
	// The solution is for the RPC handler to include a copy of the map in the reply.
	response.Shards = make(map[int]map[string]string)
	for _, shardID := range request.ShardIDs {
		response.Shards[shardID] = kv.shards[shardID].DCopyData()
	}

	response.LastOperation = make(map[int64]ClientOpRecord)
	for clientID, operation := range kv.lastOperation {
		response.LastOperation[clientID] = operation.DCopy()
	}

	response.ConfigNum = request.ConfigNum
}

// ================
//   SERVER STATE
// ================

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
