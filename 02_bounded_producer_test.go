package pq

import (
	"fmt"
	"sync"
	"testing"
)

func BenchmarkBoundedProducerPQ(b *testing.B) {
	for _, v := range concurrencyTests {
		concur := v
		b.Run(fmt.Sprintf("concurrency_%d", concur), func(b *testing.B) {
			concurrency, itemsPerGoroutine := splitTotal(b.N, concur)
			enqueueBP(concurrency, itemsPerGoroutine)
		})
	}
}

func enqueueBP(concurrency, itemsPerGoroutine int) {
	total := concurrency * itemsPerGoroutine

	wg := &sync.WaitGroup{}
	wg.Add(1)
	destQW, destQR := NewQueue[*Writer[struct{}]]()
	go func() {
		counter := 0
		for {
			completeQW := destQR.Dequeue()
			counter++
			if completeQW != nil {
				completeQW.Enqueue(struct{}{})
			}

			if counter == total {
				wg.Done()
				return
			}
		}
	}()

	const maxPendingBatch = 5
	const itemsPerBatch = 100

	for c := 0; c < concurrency; c++ {
		go func() {
			completeQW, completeQR := NewQueue[struct{}]()

			sendCounter := 0
			inflightBatch := 0
			for i := 0; i < itemsPerGoroutine; i++ {
				if inflightBatch == maxPendingBatch {
					for _, ok := completeQR.TryDequeue(); inflightBatch > 0 && ok; _, ok = completeQR.TryDequeue() {
						inflightBatch--
					}

					if inflightBatch == maxPendingBatch {
						completeQR.Dequeue()
						inflightBatch--
					}
				}

				sendCounter++
				var compQW *Writer[struct{}]
				if sendCounter%itemsPerBatch == 0 {
					compQW = completeQW
					inflightBatch++
				}

				destQW.Enqueue(compQW)
			}
		}()
	}

	wg.Wait()
}
