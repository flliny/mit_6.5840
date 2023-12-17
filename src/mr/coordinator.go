package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskStatus string

const (
	Ready    TaskStatus = "ready"
	Running  TaskStatus = "running"
	Complete TaskStatus = "complete"
)

type TaskType string

const (
	MAP    TaskType = "map"
	REDUCE TaskType = "reduce"
)

type Task struct {
	ID         int
	TType      TaskType
	TargetFile string
	Status     TaskStatus
	UpdateTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	Mu         sync.Mutex
	MTaskQueue []*Task
	RTaskQueue []*Task

	Stage   string
	NMap    int
	NReduce int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	fmt.Println("hello from mrcoordinator.")
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	if c.Stage == "map" {
		for _, task := range c.MTaskQueue {
			if task.Status == Ready || task.Status == Running {
				return false
			}
		}
		c.Stage = "reduce"
	}

	if c.Stage == "reduce" {
		for _, task := range c.RTaskQueue {
			if task.Status == Ready || task.Status == Running {
				return false
			}
		}
		c.Stage = "done"
	}

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MTaskQueue: make([]*Task, len(files)),
		RTaskQueue: make([]*Task, nReduce),
		Stage:      "map",
		NMap:       len(files),
		NReduce:    nReduce,
	}

	// Your code here.
	for i, file := range files {
		c.MTaskQueue[i] = &Task{
			ID:         i,
			TType:      MAP,
			TargetFile: file,
			UpdateTime: time.Now(),
			Status:     Ready,
		}
	}
	for i := range c.RTaskQueue {
		c.RTaskQueue[i] = &Task{
			ID:         i,
			TType:      REDUCE,
			TargetFile: "",
			UpdateTime: time.Now(),
			Status:     Ready,
		}
	}
	go c.healthCheck()

	c.server()
	return &c
}

func (c *Coordinator) healthCheck() {
	ticker := time.NewTicker(time.Second)
	var curTaskQueue []*Task
	for {
		<-ticker.C
		if c.Stage == "map" {
			curTaskQueue = c.MTaskQueue
		} else if c.Stage == "reduce" {
			curTaskQueue = c.RTaskQueue
		}
		for _, task := range curTaskQueue {
			if task.Status == Running &&
				time.Since(task.UpdateTime) > 10*time.Second {
				task.Status = Ready
				task.UpdateTime = time.Now()
			}
		}
	}
}

type GetTaskArgs struct{}
type GetTaskReply = Task

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()

	if c.Stage == "map" {
		for _, mTask := range c.MTaskQueue {
			if mTask.Status == Ready {
				mTask.Status = Running
				mTask.UpdateTime = time.Now()
				*reply = *mTask
				return nil
			}
		}
		// ID = -1 means tell worker waiting
		reply.ID = -1
		return nil
	} else if c.Stage == "reduce" {
		for _, rTask := range c.RTaskQueue {
			if rTask.Status == Ready {
				rTask.Status = Running
				rTask.UpdateTime = time.Now()
				*reply = *rTask
				return nil
			}
		}
		reply.ID = -1
		return nil
	}

	return fmt.Errorf("[DONE]")
}

type PutTaskArgs Task
type PutTaskReply bool

func (c *Coordinator) PutTask(args *PutTaskArgs, reply *PutTaskReply) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()

	var curTaskQueue []*Task
	if args.TType == MAP {
		curTaskQueue = c.MTaskQueue
	} else if args.TType == REDUCE {
		curTaskQueue = c.RTaskQueue
	}

	for _, task := range curTaskQueue {
		if task.ID == args.ID && task.UpdateTime.Equal(args.UpdateTime) {
			task.UpdateTime = time.Now()
			task.Status = args.Status
		}
	}

	return nil
}

type GetNReduceArgs struct{}
type GetNReduceReply = int

func (c *Coordinator) GetNReduce(
	args *GetNReduceArgs,
	reply *GetNReduceReply,
) error {
	*reply = c.NReduce
	return nil
}
