package kvsrv

import (
	"6.5840/labrpc"
)
import "crypto/rand"
import "math/big"

const MAXRETRY int = 10

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
	cid int64
	tid int
}

// nrand return a random non-negative int64
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	ck.cid = nrand()
	ck.tid = 0
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
	ck.tid = ck.tid + 1
	args := GetArgs{key, ck.cid}
	reply := GetReply{}
	//log.Printf("Client %v Transaction %v Get %v from server\n", ck.cid, tid, key)
	for ok := ck.server.Call("KVServer.Get", &args, &reply); !ok; ok = ck.server.Call("KVServer.Get", &args, &reply) {
		//time.Sleep(time.Millisecond * 20)
	}
	return reply.Value
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
	tid := ck.tid
	ck.tid = ck.tid + 1
	args := PutAppendArgs{key, value, tid, ck.cid}
	reply := PutAppendReply{}
	//log.Printf("Client %v Transaction %v %v %v: %v to server\n", ck.cid, tid, op, key, value)
	for ok := ck.server.Call("KVServer."+op, &args, &reply); !ok; ok = ck.server.Call("KVServer."+op, &args, &reply) {
		//time.Sleep(time.Millisecond * 20)
	}
	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
