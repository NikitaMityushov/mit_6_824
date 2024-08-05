package kvserver

import "time"

// Put or Append
type PutAppendArgs struct {
	Id    int64
	Key   string
	Value string
	Ttl   time.Duration
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Value string
}

const TTL_REQUEST time.Duration = 200 * time.Millisecond
const TTL_SIGMA time.Duration = 25 * time.Microsecond
