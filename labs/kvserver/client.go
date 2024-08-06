package kvserver

import (
	"time"

	"github.com/NikitaMityushov/mit_6_824/labs/labrpc"
	"github.com/google/uuid"
)

type Clerk struct {
	// mu               sync.Mutex
	server           *labrpc.ClientEnd
	clientId         int64
	currentRequestId int
	readRequestId    int
	// You will have to modify this struct.
}

func createUniqueClientID() int64 {
	currentTimestamp := time.Now().UnixNano() / int64(time.Microsecond)

	// Obtain a unique code using UUID
	uniqueID := uuid.New().ID()

	// Combine the time-based component and the UUID to create a globally unique ID
	ID := currentTimestamp + int64(uniqueID)
	return ID
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	uniqueClientId := createUniqueClientID()
	ck := Clerk{server: server,
		clientId:         uniqueClientId,
		currentRequestId: 0,
		readRequestId:    0}
	return &ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	reply := GetReply{}
	for {
		ok := ck.server.Call("KVServer.Get", &GetArgs{Key: key}, &reply)
		if ok {
			return reply.Value
		} else {
			continue
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	ck.currentRequestId++
	requestId := ck.currentRequestId
	reply := PutAppendReply{}
	for {
		ok := ck.server.Call(
			"KVServer."+op,
			&PutAppendArgs{ClientId: ck.clientId,
				Key:           key,
				Value:         value,
				Ttl:           TTL_REQUEST,
				RequestId:     requestId,
				ReadRequestId: ck.readRequestId},
			&reply)
		if ok {
			ck.readRequestId++
			return reply.Value
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
