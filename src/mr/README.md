# Lab 1: MapReduce
## Test Script
`main/test-mr.sh`

## Notes
### Coordinator @ MapPhase
- Purpose: initialize `Coordinator`, setup tasks, and wait for worker
- Called by: `main/mrcoordinator.go`

#### func `MakeCoordinator`
1. initial a Coordinator
    - `TaskStat.Status` is all `0`/`TaskStatusReady` in Status
    - `taskCh` is an **buffered channel** in order **not to block other goroutines**
2. `initMapTask()`
    - set the phase to `MapPhase`
    - make slice taskStats of `len(c.files)`
3. `tickSchedule()` run `schedule()` concurrently
    - `schedule()`
        - lock Coordinator
        - iterate through taskStats: push task to `taskCh` and update `taskStats[idx]` to `TaskStatusQueue`
4. Task is now available to be fetch by `Coordinator.GetOneTask`

### Worker @ MapPhase
- Called by: `main/mrworker.go`
#### func Worker()
1. initialize worker with `mapf` `reducef`
2. `register()`
    - call `Coordinator.RegWorker` to get a `WorkerId`
3. `getTask()`
    - call `Coordinator.GetOneTask` to get a `Task` by `WorkerId`
4. `doTask()` -> `doMapTask()`
    - read the file in task (one file per task)
    - use `mapf()` to do map work, generate key-value sets.
    - use `ihash()` to determine the `reduceIdx` of each file
    - write intermediate file, i.e. key-value with `ihash(key)/nReduce` of `2` of `3rd` `file` will be written into `mr-3-2` 
    - write intermediate file, will generate `nReduce` * `file_num` of files partition in `mr-tmp`

