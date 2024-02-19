package mr

import (
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	lock sync.Mutex

	R            int
	Phase        bool        // false for mapping and true for reducing
	Tasks        map[int]int // map from task index to task status
	TaskChannel  chan Task
	Workers      map[int]int
	Intermediate [][]string
}

func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
	id := args.ID
	task, ok := <-c.TaskChannel
	if !ok {
		return nil // channel closed, all tasks are done
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	reply.Task = task
	c.Workers[id] = task.TID
	go func(tid int) {
		time.Sleep(10 * time.Second)
		if _, ok := c.Tasks[tid]; ok {
			c.TaskChannel <- task
		}
	}(task.TID)
	return nil
}

func (c *Coordinator) MapTaskDone(args *MapTaskDoneArgs, reply *MapTaskDoneReply) error {
	id := args.ID
	tid := c.Workers[id]
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.Workers, id)
	if _, ok := c.Tasks[tid]; !ok {
		return nil
	}
	delete(c.Tasks, tid)
	//log.Printf("task %v done\n", tid)
	for i, path := range args.Paths {
		c.Intermediate[i] = append(c.Intermediate[i], path)
	}
	if !c.Phase && len(c.Tasks) == 0 {
		// phase change
		for i := 0; i < c.R; i++ {
			task := Task{
				TID:        i,
				MapTask:    nil,
				ReduceTask: &ReduceTask{i, c.Intermediate[i]},
			}
			c.Tasks[i] = 0
			c.TaskChannel <- task
		}
		c.Phase = true
	}
	return nil
}

func (c *Coordinator) ReduceTaskDone(args *ReduceTaskDoneArgs, reply *ReduceTaskDoneReply) error {
	id := args.ID
	tid := c.Workers[id]
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.Workers, id)
	if _, ok := c.Tasks[tid]; !ok {
		return nil
	}
	delete(c.Tasks, tid)
	//log.Printf("task %v done\n", tid)

	path := args.Path
	outpath := "mr-out-" + strconv.Itoa(tid)
	os.Rename(path, outpath)

	if len(c.Tasks) == 0 {
		close(c.TaskChannel)
	}

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
	ret := false
	log.Printf("current state %v %v\n", c.Phase, c.Tasks)
	if c.Phase && len(c.Tasks) == 0 {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		R:            nReduce,
		Phase:        false,
		Tasks:        make(map[int]int),
		TaskChannel:  make(chan Task, len(files)+nReduce),
		Workers:      make(map[int]int),
		Intermediate: make([][]string, nReduce),
	}

	// create several tasks and assign them when worker asks, thus the coordinator should save the tasks
	// and also which worker are doing which task.

	for i, filename := range files {
		// adding map tasks
		task := Task{
			TID:        i,
			MapTask:    &MapTask{filename, nReduce},
			ReduceTask: nil,
		}
		c.TaskChannel <- task
		c.Tasks[i] = 0
	}

	log.Println("tasks prepared, waiting for workers")
	c.server()
	return &c
}
