package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	var r *rand.Rand

	n.Handle("init", func(msg maelstrom.Message) error {
		var init maelstrom.InitMessageBody
		if err := json.Unmarshal(msg.Body, &init); err != nil {
			return fmt.Errorf("unmarshal init message body: %w", err)
		}

		nodeId, err := strconv.ParseInt(strings.TrimPrefix(init.NodeID, "n"), 10, 64)
		if err != nil {
			return fmt.Errorf("turning nodeId to int64: %w", err)
		}

		r = rand.New(rand.NewSource(nodeId))
		return nil
	})

	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "generate_ok"
		body["id"] = r.Uint64()

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
