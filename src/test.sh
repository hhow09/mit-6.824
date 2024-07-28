set -ex

TIMES=5
for i in $(seq 1 $TIMES); do
  echo "Running test 2A-$i"
  VERBOSE=1 go test ./raft/... -race -run 2A -count=1
done

for i in $(seq 1 $TIMES); do
  echo "Running test 2B-$i"
  VERBOSE=1 go test ./raft/... -race -run 2B -count=1
done

for i in $(seq 1 $TIMES); do
  echo "Running test 2C-$i"
  VERBOSE=1 go test ./raft/... -race -run "(TestPersist12C|TestPersist22C|TestPersist32C|TestFigure82C)" -count=1 >> log.txt
done