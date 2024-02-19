package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	data  map[string]string
	cache map[int64]struct {
		history string
		tid     int
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	key := args.Key
	cid := args.CID

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if value, ok := kv.data[key]; ok {
		reply.Value = value
	} else {
		reply.Value = ""
	}
	delete(kv.cache, cid)
	//log.Printf("get %v: %v from memory\n", key, reply.Value)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	key := args.Key
	value := args.Value
	cid := args.CID

	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.data[key] = value
	delete(kv.cache, cid)
	//log.Printf("put %v: %v into memory\n", key, reply.Value)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	key := args.Key
	value := args.Value
	cid := args.CID

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if history, ok := kv.cache[cid]; ok && history.tid == args.TID {
		reply.Value = history.history
	} else {
		old, ok := kv.data[key]
		if !ok {
			old = ""
		}
		kv.data[key] = old + value
		reply.Value = old

		kv.cache[cid] = struct {
			history string
			tid     int
		}{history: reply.Value, tid: args.TID}
	}
	//log.Printf("append %v: %v into memory\n", key, reply.Value)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.data = make(map[string]string)
	kv.cache = make(map[int64]struct {
		history string
		tid     int
	})

	return kv
}
