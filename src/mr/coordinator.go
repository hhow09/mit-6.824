package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	MaxTaskRunTime   = time.Second * 5
	ScheduleInterval = time.Millisecond * 500
)

const (
	TaskStatusReady   = 0
	TaskStatusQueue   = 1
	TaskStatusRunning = 2
	TaskStatusFinish  = 3
	TaskStatusErr     = 4
)

type TaskStat struct {
	Status int
}

type Coordinator struct {
	// Your definitions here.
	files   []string
	nReduce int

	//task
	taskPhase TaskPhase
	taskStats []TaskStat // Status of tasks of len(tasks) / len(files)
	taskCh    chan Task  // channel to pass Task

	mu        sync.Mutex
	done      bool
	workerSeq int
}

func (c *Coordinator) getTask(taskSeq int) Task {
	task := Task{
		FileName: "",
		NReduce:  c.nReduce,
		NMaps:    len(c.files),
		Seq:      taskSeq,
		Phase:    c.taskPhase,
		Alive:    true,
	}
	DPrintf("getTask, c:%+v, taskSeq:%d, lenfiles:%d, lents:%d", c, taskSeq, len(c.files), len(c.taskStats))
	if task.Phase == MapPhase {
		task.FileName = c.files[taskSeq]
	}
	return task
}

func (c *Coordinator) schedule() {
	//shedule
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.done {
		return
	}

	allFinished := true
	for index, t := range c.taskStats {
		switch t.Status {
		case TaskStatusReady:
			allFinished = false
			c.taskCh <- c.getTask(index)
			c.taskStats[index].Status = TaskStatusQueue

		case TaskStatusQueue:
			allFinished = false

		case TaskStatusRunning:
			allFinished = false

		case TaskStatusFinish:

		case TaskStatusErr:
			allFinished = false
			c.taskStats[index].Status = TaskStatusQueue
			c.taskCh <- c.getTask(index)

		default:
			panic("task status err")
		}
	}
	if allFinished {
		if c.taskPhase == MapPhase {
			c.initReduceTask()
		} else {
			c.done = true
		}
	}

}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RegWorker(args *RegisterArgs, reply *RegisterReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.workerSeq += 1
	reply.WorkerId = c.workerSeq
	return nil
}

func (c *Coordinator) GetOneTask(args *TaskArgs, reply *TaskReply) error {
	DPrintf("Coordinator.GetOneTask")
	task := <-c.taskCh
	reply.Task = &task

	return nil

}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	DPrintf("ReportTask, get report task:%+v, taskPhase:%+v", args, c.taskPhase)

	//TODO error handling

	//update taskStats: Done or Error
	// if args

	// go c.schedule()
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	//TODO why Lock
	return c.done
}

func (c *Coordinator) initMapTask() {
	c.taskPhase = MapPhase
	c.taskStats = make([]TaskStat, len(c.files)) //create TaskStat slice of length c.nReduce
}

func (c *Coordinator) initReduceTask() {
	DPrintf("init ReduceTask")
	// c.taskPhase = ReducePhase
	// c.taskStats = make([]TaskStat, c.nReduce)
}

func (c *Coordinator) tickSchedule() {
	// if not Done yet, run schedule periodically
	for !c.Done() {
		go c.schedule()
		time.Sleep(ScheduleInterval) //stop the latest go-routine for at least the stated duration
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.mu = sync.Mutex{}
	c.nReduce = nReduce
	c.files = files

	if nReduce > len(files) {
		c.taskCh = make(chan Task, nReduce)
	} else {
		c.taskCh = make(chan Task, len(c.files))
	}

	c.initMapTask()
	go c.tickSchedule()

	c.server()
	return &c
}
