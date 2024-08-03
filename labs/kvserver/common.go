package kvserver

// Put or Append
type PutAppendArgs struct {
	Id    int64
	Key   string
	Value string
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

