# Shard KV
- [Lab REQUIREMENT](./REQUIREMENT.md)

## Overview
- a `ShardKV` controlls a subset of shards
- configuration change should reach agreement among with other raft nodes.

## Command Type
- `CommandOp`: command from client, could be operation `Get`, `Put` or `Append`
- `CommandConfig`: configuration change command.
- `CommandInsertShard`: insert shard commmand

## Shard Migration
- server monitor the new config change
- when shards need to be moved around servers we need a mechanism.
- noted that intermediate state should only affect at single shard level, if shard status keep serving, it should not be affected.

### Migration Phases
```
[Phase]
    [Shard Status] -> [New Shard Status] (reason)

Update routine (upon receiving new config)
    Serving -> Pulling (new config has this shard in group)
    Serving -> PulledByOthers (shard removed from group)

Pull routine
    Pulling -> Cleaning (pull new shards)

Cleanup routine
    PulledByOthers ->  X  (Deleted)
    Cleaning -> Serving  (back to normal)
```
