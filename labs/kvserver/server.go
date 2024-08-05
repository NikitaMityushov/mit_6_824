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
	duplicates map[int64]RequestRecord
}

func (kv *KVServer) isRequestIdDuplicate(id *int64) bool {
	_, exists := kv.duplicates[*id]
	return exists
}

func (kv *KVServer) addToDuplicates(id int64, value *string, ttl time.Duration) {
	dur := time.Now().Add(ttl).Add(TTL_SIGMA)
	kv.duplicates[id] = RequestRecord{OldValue: value, AliveBefore: dur}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.Value = kv.storage[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	/*
		kv.storage[args.Key] = args.Value
		reply.Value = kv.storage[args.Key]
	*/

	isDuplicate := kv.isRequestIdDuplicate(&args.Id)
	if !isDuplicate {
		kv.storage[args.Key] = args.Value
		v := kv.storage[args.Key]
		kv.addToDuplicates(args.Id, &v, args.Ttl)
		reply.Value = v
	} else {
		reply.Value = *kv.duplicates[args.Id].OldValue
	}

}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	isDuplicate := kv.isRequestIdDuplicate(&args.Id)
	if !isDuplicate {
		v, ok := kv.storage[args.Key]
		if ok {
			kv.storage[args.Key] = v + args.Value
			kv.addToDuplicates(args.Id, &v, args.Ttl)
			reply.Value = v
		} else {
			kv.addToDuplicates(args.Id, &v, args.Ttl)
			reply.Value = ""
		}

	} else {
		v := kv.duplicates[args.Id]
		reply.Value = *v.OldValue
	}

}

func (kv *KVServer) clearDuplicates() {
	for {
		time.Sleep(250 * time.Millisecond)
		kv.mu.Lock()
		for k, v := range kv.duplicates {
			if v.AliveBefore.Before(time.Now()) {
				delete(kv.duplicates, k)
			}
		}
		kv.mu.Unlock()
	}
}

func StartKVServer() *KVServer {
	m := make(map[string]string)
	d := make(map[int64]RequestRecord)
	kv := KVServer{storage: m, duplicates: d}

	// garbage collector for duplicates
	go kv.clearDuplicates()

	// You may need initialization code here.

	return &kv
}

type RequestRecord struct {
	OldValue    *string
	AliveBefore time.Time
}
