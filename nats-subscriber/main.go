package main

import (
	"log"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	numWorkers = 10   // Número de workers para procesar en paralelo
	batchSize  = 1000 // Cantidad de mensajes por batch
)

func worker(id int, sub *nats.Subscription, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		msgs, err := sub.Fetch(batchSize, nats.MaxWait(2*time.Second))
		if err != nil {
			log.Printf("Worker %d: No more messages", id)
			return
		}

		for _, msg := range msgs {
			log.Printf("Worker %d: Processed %s", id, string(msg.Data))
			msg.Ack()
		}
	}
}

func main() {
	nc, err := nats.Connect(nats.DefaultURL, nats.Name("Subscriber"))
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		log.Fatal(err)
	}

	subject := "events.test"
	sub, err := js.PullSubscribe(subject, "EVENTS_CONSUMER")
	if err != nil {
		log.Fatal(err)
	}

	var wg sync.WaitGroup
	start := time.Now()

	// Lanzar múltiples workers para procesar en paralelo
	for i := range numWorkers {
		wg.Add(1)
		go worker(i, sub, &wg)
	}

	wg.Wait()
	log.Printf("Processed 1M messages in %v", time.Since(start))
}
