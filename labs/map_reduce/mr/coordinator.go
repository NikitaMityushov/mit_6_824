package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
)

const (
	NET_PROTOCOL = "tcp"
	HOST         = "127.0.0.1"
	SERVER_PORT  = ":8081"
)

type Coordinator struct {
	fs        []string
	doneJobs  int
	currIndex int
	gap       int
	nReduce   int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetFiles(args *GetFilesRequest, out *GetFilesResponse) error {
	newIndex := min(c.currIndex+c.gap, len(c.fs)-1)
	out.Files = c.fs[c.currIndex:newIndex]
	c.currIndex = newIndex
	c.doneJobs = c.currIndex + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen(NET_PROTOCOL, SERVER_PORT)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	fmt.Printf("Coordinator server starts on host %s, port %s\n", HOST, SERVER_PORT)

	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	res := len(c.fs) == c.doneJobs
	if res {
		fmt.Println("Done!")
	}
	return res
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		fs:      files,
		nReduce: nReduce,
		gap:     len(files) / nReduce,
	}
	// Your code here.

	c.server()
	return &c
}
