package shardkv

import "6.824/shardctrler"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	// OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	ErrStaleConfig = "ErrStaleConfig"
	ErrNotReady    = "ErrNotReady"
)

type Err string

type CommandType string

const (
	CommandOp          CommandType = "Op"
	CommandConfig      CommandType = "Config"
	CommandInsertShard CommandType = "InsertShard"
)

// implement stringer
func (c CommandType) String() string {
	return string(c)
}

type Command struct {
	Type CommandType
	Data interface{}
}

// same as lab3
type OpType uint8

// same as lab3
const (
	OpGet OpType = iota
	OpPut
	OpAppend
)

func (opType OpType) String() string {
	switch opType {
	case OpGet:
		return "Get"
	case OpPut:
		return "Put"
	case OpAppend:
		return "Append"
	}
	return "Unknown"
}

// same as lab3
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

func NewOpCommand(op Op) Command {
	return Command{
		Type: CommandOp,
		Data: op,
	}
}

func NewConfigCommand(config shardctrler.Config) Command {
	return Command{
		Type: CommandConfig,
		Data: config,
	}
}

func NewInsertShardCommand(res ShardInterServerResponse) Command {
	return Command{
		Type: CommandInsertShard,
		Data: res,
	}
}

// client - server

type ArgsCommon struct {
	ClientID  int64
	RequestID int64
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ArgsCommon
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ArgsCommon
}

type GetReply struct {
	Err   Err
	Value string
}

type ClientOpRecord struct {
	RequestID int64
	Reply     reply
}

func (c ClientOpRecord) DCopy() ClientOpRecord {
	return ClientOpRecord{
		RequestID: c.RequestID,
		Reply:     c.Reply,
	}
}

type reply struct {
	Value string
	Err   Err
}

// server - server
type ShardInterServerRequest struct {
	ConfigNum int
	ShardIDs  []int
}

type ShardInterServerResponse struct {
	ConfigNum     int
	Shards        map[int]map[string]string
	LastOperation map[int64]ClientOpRecord
	Err           Err
}
