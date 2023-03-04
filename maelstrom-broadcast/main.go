package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	s := &SafeSlice{
		mut:  new(sync.Mutex),
		vals: make([]any, 0, 1024),
	}

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var in map[string]any
		if err := json.Unmarshal(msg.Body, &in); err != nil {
			return err
		}

		s.Insert(in["message"])

		out := map[string]any{
			"type": "broadcast_ok",
		}
		return n.Reply(msg, out)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		out := map[string]any{
			"type":     "read_ok",
			"messages": s.GetCopy(),
		}
		return n.Reply(msg, out)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		out := map[string]any{
			"type": "topology_ok",
		}
		return n.Reply(msg, out)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type SafeSlice struct {
	mut  *sync.Mutex
	vals []any
}

func (r *SafeSlice) Insert(val any) {
	r.mut.Lock()
	defer r.mut.Unlock()
	r.vals = append(r.vals, val)
}

func (r *SafeSlice) GetCopy() []any {
	r.mut.Lock()
	defer r.mut.Unlock()
	c := make([]any, len(r.vals))
	copy(c, r.vals)
	return c
}
