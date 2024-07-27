package mr

import (
	"fmt"
	"log"
)

type TaskPhase int

const (
	MapPhase    TaskPhase = 0
	ReducePhase TaskPhase = 1
)

const Debug = true

const tmpFolderPath = "./mr-tmp"

func DPrintf(format string, v ...interface{}) {
	if Debug {
		log.Printf(format+"\n", v...)
	}
}

type Task struct {
	FileName string
	NMaps    int
	NReduce  int
	Seq      int
	Phase    TaskPhase
	Alive    bool
}

func reduceName(mapIdx, reduceIdx int) string {
	//%d for integer base-10
	return fmt.Sprintf("mr-%d-%d", mapIdx, reduceIdx)
}
