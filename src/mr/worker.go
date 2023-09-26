package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

var fileMutex sync.Mutex

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	re := RequestTaskReply{}

	CallRequestTask(&re, -1)
	if re.TaskType == 2 {
		log.Printf("No more tasks to do!\n")
	}
	log.Printf("Worker %v got task: %v\n", re.WorkerId, re)
	if re.TaskType == -1 {
		time.Sleep(1 * time.Second) // sleep a sec
	}
	if re.TaskType == 0 {
		MapTask(mapf, re)
	}
	if re.TaskType == 1 {
		ReduceTask(reducef, re)
	}

	for re.TaskType != 2 {
		CallRequestTask(&re, re.WorkerId)
		log.Printf("Worker %v got task: %v\n", re.WorkerId, re)
		if re.TaskType == 0 {
			MapTask(mapf, re)
		} else if re.TaskType == 1 {
			ReduceTask(reducef, re)
		} else if re.TaskType == -1 {
			time.Sleep(1 * time.Second) // sleep a sec
		}

	}
}

func CallReportTask(args ReportTaskArgs, reply *ReportTaskReply) {
	// time.Sleep(1 * time.Second)

	ok := call("Coordinator.ReportTask", &args, &reply)
	if ok {
		fmt.Printf("Got reply from coordinator: %v\n", reply)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func CallRequestTask(re *RequestTaskReply, workerId int) {
	args := RequestTaskArgs{}
	fmt.Printf("got workerId %v\n", workerId)
	if workerId == -1 {
		args.WorkerId = -1
	} else {
		args.WorkerId = workerId
	}
	reply := RequestTaskReply{}
	ok := call("Coordinator.RequestTask", &args, &reply)
	if ok {
		fmt.Printf("Got reply from coordinator: %v\n", reply)
		re.WorkerId = reply.WorkerId
		re.NReduce = reply.NReduce
		re.TaskType = reply.TaskType
		re.TaskId = reply.TaskId
		re.FileList = reply.FileList
	} else {
		fmt.Printf("call failed!\n")
	}
}

func makeFilesList(nReduce int, bucketId string) []string {
	var files []string

	for i := 0; i < nReduce; i++ {
		fileName := fmt.Sprintf("mr-%s-%d", bucketId, i)
		// fmt.Printf("Creating file %v\n", fileName)
		file, err := os.Create(fileName)
		if err != nil {
			log.Fatalf("cannot create %v", fileName)
		}
		file.Close()
		files = append(files, fileName)
	}

	return files
}

func MapTask(mapf func(string, string) []KeyValue, reply RequestTaskReply) {
	intermediate := []KeyValue{}

	for _, filename := range reply.FileList {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		// fmt.Printf("Read file %v\n", filename)
		// fmt.Print(string(content))
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}
	// sort.Sort(ByKey(intermediate))
	// fmt.Printf("%v", intermediate)
	intermediateFiles := makeFilesList(reply.NReduce, fmt.Sprintf("%d", reply.TaskId))
	for _, kv := range intermediate {
		reduceId := ihash(kv.Key) % reply.NReduce
		fileName := intermediateFiles[reduceId]
		file, err := os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY, 0644)
		fileMutex.Lock()
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}
		enc := json.NewEncoder(file)
		err2 := enc.Encode(&kv)
		if err2 != nil {
			log.Fatalf("error is %v", err2)
		}
		file.Close()
		fileMutex.Unlock()
	}
	args := ReportTaskArgs{}
	args.WorkerId = reply.WorkerId
	args.TaskId = reply.TaskId
	reply2 := ReportTaskReply{}
	fmt.Printf("Map task %v done!\n", reply.TaskId)
	CallReportTask(args, &reply2)
}

func makeReadList(nReduct int, bucketId string) []string {
	var files []string

	for i := 0; i < nReduct; i++ {
		fileName := fmt.Sprintf("mr-%d-%s", i, bucketId)
		files = append(files, fileName)
	}

	return files
}

func ReduceTask(reducef func(string, []string) string, reply RequestTaskReply) {
	intermediateFiles := makeReadList(reply.NReduce, fmt.Sprintf("%d", reply.TaskId))
	intermediate := []KeyValue{}
	for _, fileName := range intermediateFiles {
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))
	OutFile := fmt.Sprintf("mr-out-%d", reply.TaskId)
	file, err := os.Create(OutFile)
	if err != nil {
		log.Fatalf("cannot create %v", OutFile)
	}
	// write intermediate to file in %v %v format
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	file.Close()

	CallReportTask(ReportTaskArgs{reply.WorkerId, reply.TaskId}, &ReportTaskReply{})
}

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

	fmt.Println(err)
	return false
}
