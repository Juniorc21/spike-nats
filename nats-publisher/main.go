package main

import (
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {
	nc, err := nats.Connect(nats.DefaultURL, nats.Name("Publisher"))
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		log.Fatal(err)
	}

	streamName := "EVENTS"
	subject := "events.test"

	// Configurar el stream (si no existe)
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{subject},
	})
	if err != nil {
		log.Fatal(err)
	}

	start := time.Now()

	// Publicar 1 millón de mensajes en batch
	for i := range 1000000 {
		msg := fmt.Sprintf("Message #%d", i)
		_, err = js.Publish(subject, []byte(msg))
		if err != nil {
			log.Fatal(err)
		}

		// Pequeña pausa cada 10k mensajes para evitar congestión
		if i%10000 == 0 {
			log.Printf("Published %d messages", i)
			// Sleep for a short duration to avoid overwhelming the server
			time.Sleep(10 * time.Millisecond)
		}
	}

	log.Printf("Successfully published 1M messages in %v", time.Since(start))
}
