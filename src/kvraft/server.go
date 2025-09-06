package kvraft

import (
	"fmt"
	"lab5/constants"
	"lab5/labgob"
	"lab5/labrpc"
	"lab5/logger"
	"lab5/raft"
	"sync"
	"sync/atomic"
	"time"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string // "Get", "Put", or "Append"
	Key       string
	Value     string
	ClientId  int
	RequestId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	logger *logger.Logger
	// Your definitions here.
	maxraftstate int
	// Key-value database
	database map[string]string
	// Client request tracking for deduplication
	clientRequests map[int]int64
	// Channels for pending requests awaiting commitment
	pendingRequests map[int]chan Op
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// Check if we're the leader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// Create operation
	op := Op{
		Operation: "Get",
		Key:       args.Key,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	// Submit to Raft
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// Wait for commitment
	ch := kv.getNotificationChannel(index)

	select {
	case committedOp := <-ch:
		// Verify this is our operation
		if committedOp.ClientId == args.ClientId && committedOp.RequestId == args.RequestId {
			kv.mu.Lock()
			value, exists := kv.database[args.Key]
			kv.mu.Unlock()

			if exists {
				reply.Err = OK
				reply.Value = value
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}

	kv.cleanupChannel(index)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// Check if we're the leader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// Create operation
	op := Op{
		Operation: args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	// Submit to Raft
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// Wait for commitment
	ch := kv.getNotificationChannel(index)

	select {
	case committedOp := <-ch:
		// Verify this is our operation
		if committedOp.ClientId == args.ClientId && committedOp.RequestId == args.RequestId {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}

	kv.cleanupChannel(index)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// Initialize logger
	logger := logger.NewLogger(me, true, fmt.Sprintf("KVServer-%d", me), constants.KvLoggingMap)

	// Register Op struct for RPC serialization
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.logger = logger

	// Initialize data structures
	kv.database = make(map[string]string)
	kv.clientRequests = make(map[int]int64)
	kv.pendingRequests = make(map[int]chan Op)

	// Create apply channel and initialize Raft
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// Start background goroutine to process committed operations
	go kv.applyCommittedOperations()

	return kv
}

// Helpers
func (kv *KVServer) getNotificationChannel(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	ch, exists := kv.pendingRequests[index]
	if !exists {
		ch = make(chan Op, 1)
		kv.pendingRequests[index] = ch
	}
	return ch
}

func (kv *KVServer) cleanupChannel(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if ch, exists := kv.pendingRequests[index]; exists {
		close(ch)
		delete(kv.pendingRequests, index)
	}
}

// Background goroutine to process committed operations from Raft
func (kv *KVServer) applyCommittedOperations() {
	for !kv.killed() {
		msg := <-kv.applyCh

		if !msg.CommandValid {
			continue
		}

		op, ok := msg.Command.(Op)
		if !ok {
			continue
		}

		kv.mu.Lock()

		// Check for duplicate requests
		lastRequestId, seen := kv.clientRequests[op.ClientId]
		isDuplicate := seen && op.RequestId <= lastRequestId

		if !isDuplicate {
			// Apply operation to state machine
			switch op.Operation {
			case "Put":
				kv.database[op.Key] = op.Value
			case "Append":
				kv.database[op.Key] += op.Value
			case "Get":
				// Get operations don't modify state
			}
			kv.clientRequests[op.ClientId] = op.RequestId
		}

		// Notify waiting RPC handler
		if ch, exists := kv.pendingRequests[msg.CommandIndex]; exists {
			select {
			case ch <- op:
			default:
			}
		}

		kv.mu.Unlock()
	}
}
