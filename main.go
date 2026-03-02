package main

import (
	"context"
	"encoding/json"
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
	topic  = "user-register.p10"
	group  = "user-consumer-group"
)

type User struct {
	Name     string `json:"name"`
	Email    string `json:"email"`
	IsActive bool   `json:"isActive"`
}

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

// produce sends sample User messages to the topic then exits.
func produce() {
	w := &kafka.Writer{
		Addr:                   kafka.TCP(broker),
		Topic:                  topic,
		AllowAutoTopicCreation: true,
		Balancer:               &kafka.LeastBytes{},
	}
	defer w.Close()

	users := []User{
		{Name: "Alice", Email: "alice@example.com", IsActive: true},
		{Name: "Bob", Email: "bob@example.com", IsActive: false},
		{Name: "Carol", Email: "carol@example.com", IsActive: true},
		{Name: "Dave", Email: "dave@example.com", IsActive: true},
		{Name: "Eve", Email: "eve@example.com", IsActive: false},
		{Name: "Frank", Email: "frank@example.com", IsActive: true},
		{Name: "Grace", Email: "grace@example.com", IsActive: false},
		{Name: "Hank", Email: "hank@example.com", IsActive: true},
		{Name: "Ivy", Email: "ivy@example.com", IsActive: false},
		{Name: "Jack", Email: "jack@example.com", IsActive: true},
	}

	for i, u := range users {
		body, err := json.Marshal(u)
		if err != nil {
			log.Fatalf("failed to marshal user: %v", err)
		}

		msg := kafka.Message{
			Key:   []byte(fmt.Sprintf("key-%d", i+1)),
			Value: body,
		}

		if err := w.WriteMessages(context.Background(), msg); err != nil {
			log.Fatalf("failed to write message: %v", err)
		}

		fmt.Printf("produced: %s\n", body)
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
		// FetchMessage ดึง message มาแต่ยังไม่ commit
		m, err := r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				fmt.Println("\nstopped consuming")
				break
			}
			log.Printf("read error: %v", err)
			continue
		}

		var u User
		if err := json.Unmarshal(m.Value, &u); err != nil {
			log.Printf("failed to unmarshal message: %v", err)
			continue
		}

		fmt.Printf("consumed: partition=%d offset=%d key=%s | name=%s email=%s isActive=%v\n",
			m.Partition, m.Offset, m.Key, u.Name, u.Email, u.IsActive)

		// delay 3 วินาทีก่อน commit (จำลอง processing time)
		fmt.Printf("processing... (3s delay before commit)\n")
		time.Sleep(3 * time.Second)

		// commit บอก broker ว่าอ่านแล้ว
		if err := r.CommitMessages(ctx, m); err != nil {
			log.Printf("failed to commit message: %v", err)
		} else {
			fmt.Printf("committed: partition=%d offset=%d\n", m.Partition, m.Offset)
		}
	}
}
