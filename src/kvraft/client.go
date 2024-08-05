package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	ID             int64
	currentLeader  int // index of the last known leader
	requestIDCount int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.ID = nrand()
	ck.requestIDCount = 0
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	req := GetArgs{
		Key: key,
		ArgsCommon: ArgsCommon{
			ClientId:  ck.ID,
			RequestID: ck.requestIDCount,
		},
	}
	for {
		reply := GetReply{}
		ok := ck.servers[ck.currentLeader].Call("KVServer.Get", &req, &reply)
		if ok && !isRetriableError(reply.Err) {
			ck.requestIDCount++
			return reply.Value
		}
		ck.nextLeader()
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	req := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		ArgsCommon: ArgsCommon{
			ClientId:  ck.ID,
			RequestID: ck.requestIDCount,
		},
	}
	for {
		reply := PutAppendReply{}
		ok := ck.servers[ck.currentLeader].Call("KVServer.PutAppend", &req, &reply)
		if ok && !isRetriableError(reply.Err) {
			ck.requestIDCount++
			return
		}
		ck.nextLeader()
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) nextLeader() int {
	ck.currentLeader = (ck.currentLeader + 1) % len(ck.servers)
	return ck.currentLeader
}
