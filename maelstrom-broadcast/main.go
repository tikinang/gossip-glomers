package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type BroadcastMessageBody struct {
	Message uint64 `json:"message"`
}

type Gossip struct {
	Type    string   `json:"type"`
	Message uint64   `json:"message"`
	SentTo  []string `json:"sent_to"`
}

func main() {
	n := maelstrom.NewNode()

	store := &safeMap{
		mut:  new(sync.Mutex),
		vals: make(map[uint64]struct{}, 1024),
	}
	var selfId string
	var fellowIds []string

	n.Handle("init", func(msg maelstrom.Message) error {
		var in maelstrom.InitMessageBody
		if err := json.Unmarshal(msg.Body, &in); err != nil {
			return fmt.Errorf("unmarshal init message body: %w", err)
		}

		selfId = in.NodeID
		fellowIds = in.NodeIDs

		return nil
	})

	q := &queue{
		mut:  new(sync.Mutex),
		vals: make(map[uint64]Gossip, 1024),
	}
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var in BroadcastMessageBody
		if err := json.Unmarshal(msg.Body, &in); err != nil {
			return err
		}

		store.insert(in.Message)
		q.push(Gossip{
			Type:    "gossip",
			Message: in.Message,
			SentTo:  []string{selfId},
		})

		out := map[string]any{
			"type": "broadcast_ok",
		}
		return n.Reply(msg, out)
	})

	n.Handle("gossip", func(msg maelstrom.Message) error {
		var in Gossip
		if err := json.Unmarshal(msg.Body, &in); err != nil {
			return err
		}

		store.insert(in.Message)
		q.push(Gossip{
			Type:    "gossip",
			Message: in.Message,
			SentTo:  append(in.SentTo, selfId),
		})

		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		out := map[string]any{
			"type":     "read_ok",
			"messages": store.getSlice(),
		}
		return n.Reply(msg, out)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		out := map[string]any{
			"type": "topology_ok",
		}
		return n.Reply(msg, out)
	})

	closed := new(atomic.Bool)

	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			gossips := q.get()
			if closed.Load() && len(gossips) == 0 {
				break
			}
			wg := new(sync.WaitGroup)
			for message, gossip := range gossips {
				wg.Add(1)
				go func(message uint64, gossip Gossip) {
					defer wg.Done()
					failed := new(atomic.Bool)
					wg := new(sync.WaitGroup)
					for _, fellowId := range fellowIds {
						var alreadyReceived bool
						for _, nodeId := range gossip.SentTo {
							if nodeId == fellowId {
								alreadyReceived = true
								break
							}
						}
						if alreadyReceived {
							continue
						}
						wg.Add(1)
						go func(fellowId string, gossip Gossip) {
							defer wg.Done()
							ctx, cancel := context.WithTimeout(context.Background(), time.Second)
							defer cancel()
							if _, err := n.SyncRPC(ctx, fellowId, gossip); err != nil {
								failed.Store(true)
							}
						}(fellowId, gossip)
					}
					wg.Wait()
					// This could be optimized. Only try to resent failed requests.
					if !failed.Load() {
						q.ok(message)
					}
				}(message, gossip)
			}
			wg.Wait()
		}
	}()

	if err := n.Run(); err != nil {
		panic(err)
	}
	closed.Store(true)
	wg.Wait()
}

type queue struct {
	mut  *sync.Mutex
	vals map[uint64]Gossip
}

func (r *queue) get() map[uint64]Gossip {
	r.mut.Lock()
	defer r.mut.Unlock()
	m := make(map[uint64]Gossip, len(r.vals))
	for message, gossip := range r.vals {
		m[message] = gossip
	}
	return m
}

func (r *queue) ok(message uint64) {
	r.mut.Lock()
	defer r.mut.Unlock()
	delete(r.vals, message)
}

func (r *queue) push(gossip Gossip) {
	r.mut.Lock()
	defer r.mut.Unlock()
	r.vals[gossip.Message] = gossip
}

type safeMap struct {
	mut  *sync.Mutex
	vals map[uint64]struct{}
}

func (r *safeMap) insert(val uint64) {
	r.mut.Lock()
	defer r.mut.Unlock()
	r.vals[val] = struct{}{}
}

func (r *safeMap) getSlice() []uint64 {
	r.mut.Lock()
	defer r.mut.Unlock()
	s := make([]uint64, 0, len(r.vals))
	for val := range r.vals {
		s = append(s, val)
	}
	return s
}
