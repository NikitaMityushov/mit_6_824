package kvserver

import (
	"log"
	"sync"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu         sync.Mutex
	storage    map[string]string
	duplicates map[int64]*string
}

func (kv *KVServer) isRequestIdDuplicate(id int64) bool {
	_, exists := kv.duplicates[id]
	return exists
}

func (kv *KVServer) addToDuplicates(id int64, value *string) {
	kv.duplicates[id] = value
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.Value = kv.storage[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.storage[args.Key] = args.Value
	reply.Value = kv.storage[args.Key]
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	isDuplicate := kv.isRequestIdDuplicate(args.Id)
	if !isDuplicate {
		v, ok := kv.storage[args.Key]
		if ok {
			kv.storage[args.Key] = v + args.Value
			kv.addToDuplicates(args.Id, &v)
			reply.Value = v
		} else {
			kv.addToDuplicates(args.Id, &v)
			reply.Value = ""
		}

	} else {
		v := kv.duplicates[args.Id]
		reply.Value = *v
	}

}

func (kv *KVServer) clearDuplicates() {
	for {
		time.Sleep(1500 * time.Millisecond)
		kv.mu.Lock()
		r := len(kv.duplicates) * 12 / 13
		l := 0
		for k := range kv.duplicates {
			l++
			if l < r {
				delete(kv.duplicates, k)
			}
		}
		kv.mu.Unlock()
	}
}

func StartKVServer() *KVServer {
	m := make(map[string]string)
	d := make(map[int64]*string)
	kv := KVServer{storage: m, duplicates: d}

	// garbage collector for duplicates
	go kv.clearDuplicates()

	// You may need initialization code here.

	return &kv
}

