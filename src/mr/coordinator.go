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

type workingWorkers struct {
	id       int
	taskType int
	taskId   int
	activeAt time.Time
}

type Coordinator struct {
	// Your definitions here.
	mutex sync.Mutex

	nReduce int
	files   []string

	tasksMap [][]string
	tasksRed []int

	activeWorkers   []int
	activeProcesses []workingWorkers

	unAssignedMaps []int
	unAssignedReds []int

	finishedMaps []int
	finishedReds []int
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	log.Printf("Worker %v Requested Task\n", args.WorkerId)
	c.mutex.Lock()
	if args.WorkerId == -1 {
		if len(c.activeWorkers) != 0 {
			reply.WorkerId = c.activeWorkers[c.activeWorkers[len(c.activeWorkers)-1]] + 1
		} else {
			reply.WorkerId = 0
		}
	} else {
		reply.WorkerId = args.WorkerId
	}
	c.activeWorkers = append(c.activeWorkers, reply.WorkerId)

	reply.NReduce = c.nReduce

	if len(c.unAssignedMaps) > 0 {
		reply.TaskType = 0 // work on maps
		reply.TaskId = c.unAssignedMaps[0]
		reply.FileList = c.tasksMap[reply.TaskId]
		c.unAssignedMaps = c.unAssignedMaps[1:]
		c.activeProcesses = append(c.activeProcesses, workingWorkers{reply.WorkerId, 0, reply.TaskId, time.Now()})
	} else if len(c.finishedMaps) != c.nReduce {
		reply.TaskType = -1 // ask to wait
	} else if len(c.unAssignedReds) > 0 {
		reply.TaskType = 1 // work on reds
		reply.TaskId = c.unAssignedReds[0]
		c.unAssignedReds = c.unAssignedReds[1:]
		c.activeProcesses = append(c.activeProcesses, workingWorkers{reply.WorkerId, 1, reply.TaskId, time.Now()})
	} else if len(c.finishedReds) != c.nReduce {
		reply.TaskType = -1 // ask to wait
	} else {
		reply.TaskType = 2 // ask to exit
		reply.WorkerId = -1
	}
	c.mutex.Unlock()
	return nil
}

func (c *Coordinator) CheckLive() error {
	c.mutex.Lock()
	for i, worker := range c.activeProcesses {
		if time.Since(worker.activeAt) > 10*time.Second {
			if worker.taskType == 0 {
				c.unAssignedMaps = append(c.unAssignedMaps, worker.taskId)
			} else {
				c.unAssignedReds = append(c.unAssignedReds, worker.taskId)
			}
			c.activeProcesses = append(c.activeProcesses[:i], c.activeProcesses[i+1:]...)
			c.activeWorkers = append(c.activeWorkers[:i], c.activeWorkers[i+1:]...)
		}
	}
	c.mutex.Unlock()
	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	log.Printf("Worker %v Reported Task %v\n", args.WorkerId, args.TaskId)
	c.mutex.Lock()
	reply.WorkerId = args.WorkerId
	reply.TaskId = args.TaskId
	reply.Received = true

	for i, worker := range c.activeProcesses {
		if worker.id == args.WorkerId && worker.taskType == 0 && worker.taskId == args.TaskId {
			c.activeProcesses = append(c.activeProcesses[:i], c.activeProcesses[i+1:]...)
			c.finishedMaps = append(c.finishedMaps, args.TaskId)
			break
		} else if worker.id == args.WorkerId && worker.taskType == 1 && worker.taskId == args.TaskId {
			c.activeProcesses = append(c.activeProcesses[:i], c.activeProcesses[i+1:]...)
			c.finishedReds = append(c.finishedReds, args.TaskId)
			break
		}
	}
	log.Printf("Finished Maps: %v\n", c.finishedMaps)
	log.Printf("Finished Reds: %v\n", c.finishedReds)
	c.mutex.Unlock()
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
	// Your code here.
	ret := false
	if len(c.finishedReds) == c.nReduce {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nReduce = nReduce
	c.files = files
	buckets := make([][]string, nReduce)
	// divide files into buckets
	for i, file := range files {
		buckets[i%nReduce] = append(buckets[i%nReduce], file)
	}
	c.tasksMap = buckets
	fmt.Printf("%v", c.tasksMap)
	for i := 0; i < nReduce; i++ {
		c.unAssignedMaps = append(c.unAssignedMaps, i)
		c.tasksRed = append(c.tasksRed, i)
		c.unAssignedReds = append(c.unAssignedReds, i)
	}

	ticker := time.NewTicker(1 * time.Second)
	go func() {
		for range ticker.C {
			c.CheckLive()
		}
	}()

	c.server()
	return &c
}
