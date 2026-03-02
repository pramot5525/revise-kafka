package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	broker = "localhost:19092"
	topic  = "demo-topic"
	group  = "demo-group"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run main.go [produce|consume]")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "produce":
		produce()
	case "consume":
		consume()
	default:
		fmt.Println("Unknown command. Use 'produce' or 'consume'")
		os.Exit(1)
	}
}

// produce sends 5 messages to the topic then exits.
func produce() {
	w := &kafka.Writer{
		Addr:                   kafka.TCP(broker),
		Topic:                  topic,
		AllowAutoTopicCreation: true,
		Balancer:               &kafka.LeastBytes{},
	}
	defer w.Close()

	for i := 1; i <= 5; i++ {
		msg := kafka.Message{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("hello from redpanda – message %d", i)),
		}

		err := w.WriteMessages(context.Background(), msg)
		if err != nil {
			log.Fatalf("failed to write message: %v", err)
		}

		fmt.Printf("produced: %s\n", msg.Value)
		time.Sleep(500 * time.Millisecond)
	}

	fmt.Println("done producing")
}

// consume reads messages from the topic until SIGINT / SIGTERM.
func consume() {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{broker},
		Topic:    topic,
		GroupID:  group,
		MinBytes: 1,
		MaxBytes: 10e6, // 10 MB
		MaxWait:  time.Second,
	})
	defer r.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	fmt.Println("consuming messages (ctrl+c to stop)…")

	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				fmt.Println("\nstopped consuming")
				break
			}
			log.Printf("read error: %v", err)
			continue
		}

		fmt.Printf("consumed: partition=%d offset=%d key=%s value=%s\n",
			m.Partition, m.Offset, m.Key, m.Value)
	}
}
