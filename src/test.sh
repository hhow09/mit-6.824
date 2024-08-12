set -ex

TIMES=10
# raft
VERBOSE=1 go test ./raft/... -race -run 2A -count="$TIMES" -failfast -timeout="$((TIMES*5))m"
VERBOSE=1 go test ./raft/... -race -run 2B -count="$TIMES" -failfast -timeout="$((TIMES*5))m"
VERBOSE=1 go test ./raft/... -race -run 2C -count="$TIMES" -failfast -timeout="$((TIMES*5))m"
VERBOSE=1 go test ./raft/... -race -run 2D -count="$TIMES" -failfast -timeout="$((TIMES*5))m"
# kvraft
VERBOSE=1 go test ./kvraft/... -race -run=3A -count="$TIMES" -failfast -timeout="$((TIMES*5))m"
VERBOSE=1 go test ./kvraft/... -race -run="(TestSnapshotRPC3B|TestSnapshotSize3B|TestSpeed3B|TestSnapshotRecover3B|TestSnapshotRecoverManyClients3B|TestSnapshotUnreliable3B|TestSnapshotUnreliableRecover3B)" -count="$TIMES" -failfast -timeout="$((TIMES*5))m" 

