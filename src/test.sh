set -e
rm -f log.txt

TIMES=10
for i in $(seq 1 $TIMES); do
  echo "Running test 2A-$i"
  VERBOSE=1 go test ./raft/... -race -run 2A -count=1 >> log.txt
done

for i in $(seq 1 $TIMES); do
  echo "Running test 2B-$i"
  VERBOSE=1 go test ./raft/... -race -run 2B -count=1 >> log.txt
done

