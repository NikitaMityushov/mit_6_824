package kvserver

import (
	"log"
	"sync"
	"time"
)

const Debug = false
const TIME_FOR_GARBAGE_COLLECTOR = 500 * time.Millisecond

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

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.Value = kv.storage[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, isDuplicate := kv.isRequestIdDuplicate(&args.ClientId, &args.RequestId)
	if !isDuplicate {
		kv.storage[args.Key] = args.Value
		v := kv.storage[args.Key]
		/*
			Small optimization: unlike with append, when duplicating a put request,
			there is no need to save the old value; it is enough to save the fact of
			duplication. In our case, we store the duplication information as a nil
			value in the duplicates map, and we can simply retrieve the old value
			from the storage map.
		*/
		kv.addToDuplicates(args.ClientId, nil, args.Ttl, args.RequestId)
		reply.Value = v
	} else {
		v := kv.storage[args.Key]
		reply.Value = v
	}

}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	oldValue, isDuplicate := kv.isRequestIdDuplicate(&args.ClientId, &args.RequestId)
	if !isDuplicate {
		v, ok := kv.storage[args.Key]
		if ok {
			kv.storage[args.Key] = v + args.Value
			kv.addToDuplicates(args.ClientId, &v, args.Ttl, args.RequestId)
			reply.Value = v
		} else {
			kv.addToDuplicates(args.ClientId, &v, args.Ttl, args.RequestId)
			reply.Value = ""
		}

	} else {
		reply.Value = *oldValue
	}

}

func (kv *KVServer) isRequestIdDuplicate(id *int64, requestId *int) (*string, bool) {
	currentRecord, exists := kv.duplicates[*id]
	if exists {
		if currentRecord.RequestId >= *requestId {
			return currentRecord.OldValue, true
		} else {
			return nil, false
		}
	} else {
		return nil, false
	}
}

func (kv *KVServer) addToDuplicates(clientId int64, value *string, ttl time.Duration, requestId int) {
	aliveBefore := time.Now().Add(ttl)
	kv.duplicates[clientId] = RequestRecord{OldValue: value, RequestId: requestId, AliveBefore: aliveBefore}
}

/*
Garbage collector for duplicate requests. It is a separate goroutine that
periodically checks the duplicates map and removes the entries that have
expired.
*/
func (kv *KVServer) clearDuplicates() {
	for {
		time.Sleep(TIME_FOR_GARBAGE_COLLECTOR)
		kv.mu.Lock()
		for k, v := range kv.duplicates {
			if time.Now().After(v.AliveBefore) {
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
	// garbage collector for duplicate requests
	go kv.clearDuplicates()

	return &kv
}

type RequestRecord struct {
	OldValue    *string
	RequestId   int
	AliveBefore time.Time
}
