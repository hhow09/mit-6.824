package shardctrler

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// Shards:  shard is a subset of the key/value pairs

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK         = "OK"
	ErrTimeout = "TIMEOUT"
)

type Err string

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
	ArgsCommon
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs []int
	ArgsCommon
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int
	ArgsCommon
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number
	ArgsCommon
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

type ArgsCommon struct {
	ClientID  int64
	RequestID int64
}

type ClientOpRecord struct {
	RequestID int64
	Reply     reply
}

type reply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
