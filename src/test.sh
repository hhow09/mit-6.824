set -ex

TIMES=10
# raft
VERBOSE=1 go test ./raft/... -race -run 2A -count="$TIMES" -failfast -timeout="$((TIMES*5))m"
VERBOSE=1 go test ./raft/... -race -run 2B -count="$TIMES" -failfast -timeout="$((TIMES*5))m"
VERBOSE=1 go test ./raft/... -race -run 2C -count="$TIMES" -failfast -timeout="$((TIMES*5))m"
VERBOSE=1 go test ./raft/... -race -run 2D -count="$TIMES" -failfast -timeout="$((TIMES*5))m"
# kvraft
VERBOSE=1 go test ./kvraft/... -race -run=3A -count="$TIMES" -failfast -timeout="$((TIMES*5))m"
VERBOSE=1 go test ./kvraft/... -race -run=3B -count="$TIMES" -failfast -timeout="$((TIMES*5))m" 

# shardctrler
VERBOSE=1 go test ./shardctrler/... -race -count="$TIMES" -failfast -timeout="$((TIMES*5))m"