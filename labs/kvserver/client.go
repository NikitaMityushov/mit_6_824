package kvserver

import (
	"crypto/rand"
	"math/big"
	"time"

	"github.com/NikitaMityushov/mit_6_824/labs/labrpc"
	"github.com/google/uuid"
)

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func createUniqueID() int64 {
	currentTimestamp := time.Now().UnixNano() / int64(time.Microsecond)

	// Obtain a unique code using UUID
	uniqueID := uuid.New().ID()

	// Combine the time-based component and the UUID to create a globally unique ID
	ID := currentTimestamp + int64(uniqueID)
	return ID
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	return ck
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
	id := createUniqueID()
	reply := PutAppendReply{}
	for {
		ok := ck.server.Call("KVServer."+op, &PutAppendArgs{Id: id, Key: key, Value: value}, &reply)
		if ok {
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

