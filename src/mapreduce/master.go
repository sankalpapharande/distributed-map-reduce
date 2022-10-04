package mapreduce

import (
	"container/list"
	"sync"
)
import "fmt"

type WorkerInfo struct {
	address   string
	available bool
	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	//Your code here
	var mapJobWaitGroup sync.WaitGroup
	var reduceJobWaitGroup sync.WaitGroup
	numberOfMapJobs := mr.nMap
	numberOfReduceJobs := mr.nReduce
	for i := 0; i < numberOfMapJobs; i++ {
		mapJobWaitGroup.Add(1)
		i := i
		go func() {
			workerAddress := <-mr.registerChannel
			args := &DoJobArgs{}
			args.File = mr.file
			args.Operation = "Map"
			args.JobNumber = i
			args.NumOtherPhase = numberOfReduceJobs
			var reply DoJobReply
			ok := call(workerAddress, "Worker.DoJob", args, &reply)
			if ok == false {
				for {
					newWorker := <-mr.registerChannel
					ok := call(newWorker, "Worker.DoJob", args, &reply)
					if ok {
						mapJobWaitGroup.Done()
						mr.registerChannel <- newWorker
						break
					}
				}
			} else {
				mapJobWaitGroup.Done()
				mr.registerChannel <- workerAddress
			}
		}()

	}
	mapJobWaitGroup.Wait()

	for i := 0; i < numberOfReduceJobs; i++ {
		reduceJobWaitGroup.Add(1)
		i := i
		go func() {
			workerAddress := <-mr.registerChannel
			args := &DoJobArgs{}
			args.File = mr.file
			args.Operation = "Reduce"
			args.JobNumber = i
			args.NumOtherPhase = numberOfMapJobs
			var reply DoJobReply
			ok := call(workerAddress, "Worker.DoJob", args, &reply)
			if ok == false {
				for {
					newWorker := <-mr.registerChannel
					ok := call(newWorker, "Worker.DoJob", args, &reply)
					if ok {
						reduceJobWaitGroup.Done()
						mr.registerChannel <- newWorker
						break
					}
				}
			} else {
				reduceJobWaitGroup.Done()
				mr.registerChannel <- workerAddress
			}
		}()
	}
	reduceJobWaitGroup.Wait()
	return mr.KillWorkers()
}
