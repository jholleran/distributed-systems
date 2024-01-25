package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false
const Trace = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func TPrintf(format string, a ...interface{}) (n int, err error) {
	if Trace {
		log.Printf(format, a...)
	}
	return
}

const TIMEOUT_SECONDS = 3 * time.Second

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
	request map[string]Op
	changed chan Op
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	TPrintf("KV:%d Get request: %v", kv.me, args)

	kv.mu.Lock()

	if kv.request[args.ClientId].RequestId == args.RequestId {
		reply.Value = kv.request[args.ClientId].Value
		kv.mu.Unlock()
		return
	}

	kv.mu.Unlock()

	// If Leader then pass the GET command
	op := Op{ClientId: args.ClientId, RequestId: args.RequestId, Type: "Get", Key: args.Key}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = "Not a Leader"
		TPrintf("KV:%d Unabled to get key as its not a Leader", kv.me)
		return
	}

	TPrintf("KV:%d Get operation submitting and waiting for it to be applied: %v", kv.me, op)

	// wait for command to be applied
	ok, err := kv.waitFor(op)

	if !ok {
		reply.Err = err
		return
	}

	kv.mu.Lock()
	reply.Value = kv.request[op.ClientId].Value
	DPrintf("KV:%d Get:%v Reply:%v", kv.me, op, reply)
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	TPrintf("KV:%d Put append request: %v", kv.me, args)

	kv.mu.Lock()

	if kv.request[args.ClientId].RequestId == args.RequestId {
		kv.mu.Unlock()
		return
	}

	kv.mu.Unlock()

	// If Leader then pass the Op command
	op := Op{ClientId: args.ClientId, RequestId: args.RequestId, Type: args.Op, Key: args.Key, Value: args.Value}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = "Not a Leader"
		TPrintf("KV:%d Unabled to put key as its not a Leader", kv.me)
		return
	}

	TPrintf("KV:%d Put append operation submitting and waiting for it to be applied: %v", kv.me, args)

	// wait for command to be applied
	ok, err := kv.waitFor(op)

	if !ok {
		reply.Err = err
		return
	}

	DPrintf("KV:%d Put:%v", kv.me, op)
}

func (kv *KVServer) waitFor(expect Op) (bool, Err) {
	kv.mu.Lock()
	kv.changed = make(chan Op)
	kv.mu.Unlock()

	v := Op{}
	for v != expect {
		select {
		case v = <-kv.changed:

			//TODO: no longer a leader case
			// different request has appeared at the index returned by Start(), or that Raft's term has changed

		case <-time.After(TIMEOUT_SECONDS):
			TPrintf("KV:%d Get request timed out waiting for:%v", kv.me, expect)
			return false, "request timed out"
		}
	}

	kv.mu.Lock()
	kv.changed = nil
	kv.mu.Unlock()
	return true, ""
}

func (kv *KVServer) applyChannelListener() {
	for rep := range kv.applyCh {

		if rep.Command == nil {
			continue
		}

		op, ok := rep.Command.(Op)
		if rep.CommandValid && ok {
			TPrintf("KV:%d Apply Command received: %v, current value: %v", kv.me, rep, kv.state[op.Key])
			kv.applyOperation(op)
		} else {
			panic(fmt.Sprintf("Dropping invalid operation: %v", rep))
		}
	}
}

func (kv *KVServer) applyOperation(op Op) {
	//TPrintf("KV:%d Apply Command received: %v", kv.me, op)

	opCopy := Op{ClientId: op.ClientId, RequestId: op.RequestId, Type: op.Type, Key: op.Key, Value: op.Value}

	kv.mu.Lock()

	// Check to see of this operation was applied already
	if kv.request[op.ClientId] == op {
		TPrintf("KV:%d Apply Command has already been appled and will be ignored: %v", kv.me, op)
	} else {
		if op.Type == "Put" {
			kv.state[op.Key] = op.Value
		} else if op.Type == "Append" {
			current := kv.state[op.Key]
			kv.state[op.Key] = current + op.Value
		} else if op.Type == "Get" {
			// As the get is applied it can read state for the value
			opCopy.Value = kv.state[op.Key]
		} else {
			panic("Unknown operation: " + op.Type)
		}

		// Save the last request received by the client
		kv.request[op.ClientId] = opCopy
	}

	notifyListener := kv.changed != nil
	kv.mu.Unlock()

	if notifyListener {
		kv.changed <- op
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
	kv.request = make(map[string]Op)

	// Start the Apply Channel listener
	go kv.applyChannelListener()

	DPrintf("KV:%d Server started", kv.me)

	return kv
}
