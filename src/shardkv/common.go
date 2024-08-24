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
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	ErrStaleConfig = "ErrStaleConfig"
)

type Err string

type CommandType string

const (
	CommandOp     CommandType = "CommandOp"
	CommandConfig CommandType = "CommandConfig"
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
type reply struct {
	Value string
	Err   Err
}
