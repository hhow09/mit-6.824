package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//

type worker struct {
	id      int
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	w := worker{
		mapf:    mapf,
		reducef: reducef,
	}
	w.register()
	w.run()
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func (w *worker) register() {
	args := &RegisterArgs{}
	reply := &RegisterReply{}
	if ok := call("Coordinator.RegWorker", args, reply); !ok {
		log.Fatal("reg fail")
	}
	w.id = reply.WorkerId
}

func (w *worker) run() {
	DPrintf("worker run")

	for {
		t := w.reqTask()
		if !t.Alive {
			DPrintf("worker get task not alive return")
			return
		}
		w.doTask(t)
	}
}

func (w *worker) reqTask() Task {
	args := TaskArgs{
		WorkerId: w.id,
	}
	reply := TaskReply{}

	if ok := call("Coordinator.GetOneTask", &args, &reply); !ok {
		DPrintf("worker get task failed, exit")
		os.Exit(1)
	}

	DPrintf("worker get task:%+v", reply.Task)
	return *reply.Task
}

func (w *worker) doTask(t Task) {
	DPrintf("worker.doTask")

	switch t.Phase {
	case MapPhase:
		w.doMapTask(t)
	case ReducePhase:
		w.doReduceTask(t)
	default:
		panic(fmt.Sprintf("task phase error:%v", t.Phase))
	}
}

func (w *worker) doMapTask(t Task) {
	contents, err := ioutil.ReadFile(t.FileName)
	if err != nil {
		w.reportTask(t, false, err)
		return
	}

	kvs := w.mapf(t.FileName, string(contents))
	reduces := make([][]KeyValue, t.NReduce)

	for _, kv := range kvs {
		idx := ihash(kv.Key) % t.NReduce
		reduces[idx] = append(reduces[idx], kv)
	}

	for idx, el := range reduces {
		fileName := reduceName(t.Seq, idx)

		//create files inside folderPath
		newFile, err := os.Create(filepath.Join(tmpFolderPath, fileName))
		if err != nil {
			w.reportTask(t, false, err)
			return
		}
		encoder := json.NewEncoder(newFile)
		for _, kv := range el {
			if err := encoder.Encode(&kv); err != nil {
				w.reportTask(t, false, err)
			}
		}
		if err := newFile.Close(); err != nil {
			w.reportTask(t, false, err)
		}
	}
	w.reportTask(t, true, nil)

}

func (w *worker) doReduceTask(t Task) {
	maps := make(map[string][]string) // Declare an empty key-value map
	for i := 0; i < t.NMaps; i++ {
		fileName := filepath.Join(tmpFolderPath, reduceName(i, t.Seq))
		file, err := os.Open(fileName)
		if err != nil {
			w.reportTask(t, false, err)
			return
		}
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			if _, exist := maps[kv.Key]; !exist {
				maps[kv.Key] = make([]string, 0, 100)
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
			// { key1: [1,1,1,1,1], key2:[1,1,2,1,1], ... }
		}
	}
	res := make([]string, 0, 100)
	for k, v := range maps {
		res = append(res, fmt.Sprintf("%v %v\n", k, w.reducef(k, v)))
	}
	if err := ioutil.WriteFile(mergeName(t.Seq), []byte(strings.Join(res, "")), 0600); err != nil {
		w.reportTask(t, false, err)
	}
	w.reportTask(t, true, nil)
}

func (w *worker) reportTask(t Task, done bool, err error) {
	if err != nil {
		log.Printf("%v", err)
	}
	args := ReportTaskArgs{
		Done:     done,
		Seq:      t.Seq,
		Phase:    t.Phase,
		WorkerId: w.id,
	}
	reply := ReportTaskReply{}
	if ok := call("Coordinator.ReportTask", &args, &reply); !ok {
		DPrintf("report task failed:%+v", args)
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Printf("error in call %s: %v\n", rpcname, err)
	return false
}
