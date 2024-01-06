package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  string
	RequestId string
	Type      string // Get, "Put" or "Append"
	Key       string
	Value     string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	state   map[string]string
	request map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	DPrintf("KV:%d Get request: %v", kv.me, args)

	// Check if this is the Leader
	//if _, isLeader := kv.rf.GetState(); !isLeader {
	//	reply.Err = "Not a Leader"
	//	return
	//}

	// If Leader then pass the GET command
	op := Op{ClientId: args.ClientId, RequestId: args.RequestId, Type: "Get", Key: args.Key}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = "Not a Leader"
		DPrintf("KV:%d Unabled to get key as its not a Leader", kv.me)
		return
	}

	DPrintf("KV:%d Get operation submitting and waiting for it to be applied: %v", kv.me, args)

	// wait for command to be applied
	startTime := time.Now()
	timeout := startTime.Add(time.Duration(5) * time.Second)
	for {
		time.Sleep(time.Duration(20) * time.Millisecond)

		kv.mu.Lock()
		if kv.request[args.ClientId] == args.RequestId {
			reply.Value = kv.state[args.Key]
			DPrintf("KV:%d Command applied: %v", kv.me, op)
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()

		if time.Now().After(timeout) {
			reply.Err = "request timed out"
			DPrintf("KV:%d Get request timed out", kv.me)
			return
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	DPrintf("KV:%d Put append request: %v", kv.me, args)

	// Check if this is the Leader
	//if _, isLeader := kv.rf.GetState(); !isLeader {
	//	reply.Err = "Not a Leader"
	//	return
	//}

	// If Leader then pass the Op command
	op := Op{ClientId: args.ClientId, RequestId: args.RequestId, Type: args.Op, Key: args.Key, Value: args.Value}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = "Not a Leader"
		DPrintf("KV:%d Unabled to put key as its not a Leader", kv.me)
		return
	}

	DPrintf("KV:%d Put append operation submitting and waiting for it to be applied: %v", kv.me, args)

	// wait for command to be applied
	startTime := time.Now()
	timeout := startTime.Add(time.Duration(5) * time.Second)
	for {
		time.Sleep(time.Duration(20) * time.Millisecond)

		kv.mu.Lock()
		if kv.request[args.ClientId] == args.RequestId {
			DPrintf("KV:%d Command applied: %v", kv.me, op)
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()

		if time.Now().After(timeout) {
			reply.Err = "request timed out"
			DPrintf("KV:%d Put request timed out", kv.me)
			return
		}
	}
}

func (kv *KVServer) applyChannelListener() {
	for rep := range kv.applyCh {
		if rep.CommandValid {
			op, ok := rep.Command.(Op)
			if ok {
				DPrintf("KV:%d Apply Command received: %v", kv.me, op)

				kv.mu.Lock()
				if op.Type == "Put" {
					kv.state[op.Key] = op.Value
				} else if op.Type == "Append" {
					current := kv.state[op.Key]
					kv.state[op.Key] = current + op.Value
				} else if op.Type == "Get" {
					// ignore
				} else {
					panic("Unknown operation: " + op.Type)
				}

				// Save the last request received by the client
				kv.request[op.ClientId] = op.RequestId
				kv.mu.Unlock()
			}
		} else {
			panic("Dropping invalid operation")
		}
	}
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
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.state = make(map[string]string)
	kv.request = make(map[string]string)

	// Start the Apply Channel listener
	go kv.applyChannelListener()

	return kv
}
