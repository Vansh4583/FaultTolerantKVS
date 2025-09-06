package kvraft

import (
	"crypto/rand"
	"lab5/constants"
	"lab5/labrpc"
	"lab5/logger"
	"math/big"
	"sync/atomic"
	"time"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	logger  *logger.Logger
	// You will have to modify this struct.
	clerkId   int
	requestId int64 // for generating unique request IDs
	leaderId  int   // cache last known leader
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clerkId = int(nrand())
	ck.logger = logger.NewLogger(ck.clerkId, true, "Clerk", constants.KvLoggingMap)
	ck.leaderId = 0
	ck.requestId = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	args := GetArgs{
		Key:       key,
		ClientId:  ck.clerkId,
		RequestId: atomic.AddInt64(&ck.requestId, 1),
	}

	for {
		var reply GetReply
		server := ck.leaderId
		ok := ck.servers[server].Call("KVServer.Get", &args, &reply)

		if ok {
			if reply.Err == OK {
				return reply.Value
			} else if reply.Err == ErrNoKey {
				return ""
			}
			// If ErrWrongLeader, try next server
		}

		// Move to next server
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(50 * time.Millisecond)
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.clerkId,
		RequestId: atomic.AddInt64(&ck.requestId, 1),
	}

	for {
		var reply PutAppendReply
		server := ck.leaderId
		ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)

		if ok && reply.Err == OK {
			return
		}

		// Move to next server
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(50 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
