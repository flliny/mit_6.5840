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
	"time"
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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		var args GetNReduceArgs
		var reply GetTaskReply
		ok := call("Coordinator.GetTask", &args, &reply)
		if !ok {
			break
		}
		if reply.ID == -1 {
			time.Sleep(time.Second)
			continue
		}

		if reply.TType == MAP {
			if err := doMapTask(&reply, mapf); err != nil {
				log.Fatalf("do map task failed.")
			}
		} else if reply.TType == REDUCE {
			if err := doReduceTask(&reply, reducef); err != nil {
				log.Fatalf("do reduce task failed.")
			}
		}

		reply.Status = Complete
		var putTaskReply PutTaskReply
		ok = call("Coordinator.PutTask", reply, &putTaskReply)
		if !ok {
			log.Fatalf("put task failed!")
		}
	}
}

func doMapTask(task *Task, mapf func(string, string) []KeyValue) error {
	var args GetNReduceArgs
	var nreduce GetNReduceReply
	ok := call("Coordinator.GetNReduce", &args, &nreduce)
	if !ok {
		return fmt.Errorf("failed to call GetNReduce")
	}

	file, err := os.Open(task.TargetFile)
	if err != nil {
		return err
	}
	defer file.Close()

	content, err := ioutil.ReadAll(file)
	if err != nil {
		return err
	}

	kva := mapf(task.TargetFile, string(content))
	hashedKVa := make([][]KeyValue, nreduce)
	for _, kv := range kva {
		idx := ihash(kv.Key) % nreduce
		hashedKVa[idx] = append(hashedKVa[idx], kv)
	}

	for idx, kva := range hashedKVa {
		tmp, err := ioutil.TempFile("/root/files", "*.json")
		if err != nil {
			return err
		}
		enc := json.NewEncoder(tmp)
		for _, kv := range kva {
			err = enc.Encode(kv)
			if err != nil {
				return err
			}
		}
		os.Rename(tmp.Name(), fmt.Sprintf("/root/files/mr-%d-%d", task.ID, idx))
	}
	return nil
}

func doReduceTask(task *Task, reducef func(string, []string) string) error {
	pattern := fmt.Sprintf("/root/files/mr-*-%d", task.ID)
	files, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}

	kvaMap := make(map[string][]string)
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			return err
		}
		defer file.Close()
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvaMap[kv.Key] = append(kvaMap[kv.Key], kv.Value)
		}
	}

	tmp, err := ioutil.TempFile("/root/files", "mr-out-*")
	if err != nil {
		return err
	}
	for key, values := range kvaMap {
		output := reducef(key, values)
		fmt.Fprintf(tmp, "%v %v\n", key, output)
	}
	err = os.Rename(tmp.Name(), fmt.Sprintf("mr-out-%d", task.ID))
	if err != nil {
		return err
	}

	return nil
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
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

	fmt.Println(err)
	return false
}
