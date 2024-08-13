package kvraft

const (
	OK           = "OK"
	ErrNoKey     = "ErrNoKey"
	ErrNotLeader = "NOT_LEADER"
	ErrTimeout   = "TIMEOUT"
)

type Err string

func isRetriableError(err Err) bool {
	return (err == ErrNotLeader || err == ErrTimeout)
}

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
