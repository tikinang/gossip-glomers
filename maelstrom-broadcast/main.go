package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"

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

	distribute := make(chan Gossip, 64)
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var in BroadcastMessageBody
		if err := json.Unmarshal(msg.Body, &in); err != nil {
			return err
		}

		store.insert(in.Message)
		distribute <- Gossip{
			Type:    "gossip",
			Message: in.Message,
			SentTo:  []string{selfId},
		}

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
		distribute <- Gossip{
			Type:    "gossip",
			Message: in.Message,
			SentTo:  append(in.SentTo, selfId),
		}

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

	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for gossip := range distribute {
			for _, fellowId := range fellowIds {
				var alreadyReceived bool
				for _, nodeId := range gossip.SentTo {
					if nodeId == fellowId {
						alreadyReceived = true
						break
					}
				}
				if !alreadyReceived {
					_ = n.Send(fellowId, gossip)
				}
			}
		}
	}()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
	close(distribute)
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
