package kvraft

import (
	"6.5840/labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id                string
	lastKnownLeaderId int
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
	// You'll have to add code here.

	ck.lastKnownLeaderId = -1
	ck.id = randstring(20)

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

	// You will have to modify this function.

	args := GetArgs{ClientId: ck.id, RequestId: randstring(20), Key: key}
	for {

		if ck.lastKnownLeaderId != -1 {
			result, done := ck.GetFromServer(args, ck.lastKnownLeaderId)
			if done {
				return result
			}
		}

		for j := 0; j < len(ck.servers); j++ {
			result, done := ck.GetFromServer(args, j)
			if done {
				return result
			}
		}
		time.Sleep(time.Duration(50) * time.Millisecond)
	}

	return ""
}

func (ck *Clerk) GetFromServer(args GetArgs, serverId int) (string, bool) {
	reply := GetReply{}
	//log.Printf("Clerk get key:[%v] from server:%d", key, serverId)
	if ok := ck.servers[serverId].Call("KVServer.Get", &args, &reply); ok && len(reply.Err) == 0 {
		DPrintf("Clerk found key:[%v] with value:[%v] from server:%d", args.Key, reply.Value, serverId)
		ck.lastKnownLeaderId = serverId
		return reply.Value, true
	} else {
		DPrintf("Clerk get failed to server:%d. Reply: %v", serverId, reply)
		ck.lastKnownLeaderId = -1
	}

	return "", false
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

	args := PutAppendArgs{ClientId: ck.id, RequestId: randstring(20), Key: key, Value: value, Op: op}

	if ck.lastKnownLeaderId != -1 {
		done := ck.PutAppendFromServer(args, ck.lastKnownLeaderId)
		if done {
			return
		}
	}

	for {
		for j := 0; j < len(ck.servers); j++ {
			if ck.PutAppendFromServer(args, j) {
				return
			}
		}
		time.Sleep(time.Duration(50) * time.Millisecond)
	}
}

func (ck *Clerk) PutAppendFromServer(args PutAppendArgs, serverId int) bool {
	reply := PutAppendReply{}
	//log.Printf("Clerk put append key:[%v], value:[%v] from server:%d", key, value, serverId)
	if ok := ck.servers[serverId].Call("KVServer.PutAppend", &args, &reply); ok && len(reply.Err) == 0 {
		DPrintf("Clerk put append key:[%v], value:[%v] to server:%d", args.Key, args.Value, serverId)
		ck.lastKnownLeaderId = serverId
		return true
	} else {
		DPrintf("Clerk put append failed to server:%d. Reply: %v", serverId, reply)
		ck.lastKnownLeaderId = -1
	}

	return false
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
