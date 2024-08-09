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
	Status    int
	StartTime time.Time
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
	DPrintf("getTask, taskSeq:%d, lenfiles:%d, lents:%d", taskSeq, len(c.files), len(c.taskStats))
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

			//handle worker crash
			if time.Since(t.StartTime) > MaxTaskRunTime {
				c.taskStats[index].Status = TaskStatusQueue
				c.taskCh <- c.getTask(index)
			}

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
			DPrintf("ReducePhase allFinished")
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
	DPrintf("Coordinator.GetOneTask %+v", args)
	// if task channel is empty, worker calling GetOneTask will wait here until there is new task comes in.
	// It is called when only some worker is done
	task := <-c.taskCh
	reply.Task = &task
	if task.Alive {
		c.regTask(&task)
	}

	return nil
}

func (c *Coordinator) regTask(task *Task) {
	c.mu.Lock()
	defer c.mu.Unlock()
	DPrintf("Coordinator.regTask")

	if task.Phase != c.taskPhase {
		panic("task phase != Coordinator phase")
	}
	c.taskStats[task.Seq].Status = TaskStatusRunning
	c.taskStats[task.Seq].StartTime = time.Now()
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	DPrintf("ReportTask, get report task:%+v, taskPhase:%+v", args, c.taskPhase)

	// update taskStats: Done or Error
	if c.taskPhase != args.Phase {
		return nil
	}

	if args.Done {
		c.taskStats[args.Seq].Status = TaskStatusFinish
	} else {
		c.taskStats[args.Seq].Status = TaskStatusErr
	}

	go c.schedule()
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
	return c.done
}

func (c *Coordinator) initMapTask() {
	c.taskPhase = MapPhase
	c.taskStats = make([]TaskStat, len(c.files)) //create TaskStat slice of length c.nReduce
}

func (c *Coordinator) initReduceTask() {
	c.taskPhase = ReducePhase
	c.taskStats = make([]TaskStat, c.nReduce)
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

	//make an buffered channel in order not to block other goroutines
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
