package pq

import (
	"fmt"
	"sync"
	"testing"
)

// var concurrencyTests = []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 50, 100, 1000, 10_000, 100_000, 1_000_000}
var concurrencyTests = []int{1, 2, 3, 4, 8, 16, 1000, 100_000}

func splitTotal(total, concurrency int) (int, int) {
	if concurrency > total {
		concurrency = total
	}
	items := total / concurrency
	return concurrency, items

}

func BenchmarkEnqueuePQueue(b *testing.B) {
	//s := unsafe.Sizeof(Writer[payloadX]{})
	//_ = s
	for _, v := range concurrencyTests {
		concur := v
		b.Run(fmt.Sprintf("concurrency_%d", concur), func(b *testing.B) {
			concurrency, itemsPerGoroutine := splitTotal(b.N, concur)
			enqueuePQ(concurrency, itemsPerGoroutine)
		})
	}
}

func BenchmarkEnqueueChUnbound(b *testing.B) {
	for _, v := range concurrencyTests {
		concur := v
		b.Run(fmt.Sprintf("concurrency_%d", concur), func(b *testing.B) {
			concurrency, itemsPerGoroutine := splitTotal(b.N, concur)
			enqueueCh(concurrency, itemsPerGoroutine, concurrency*itemsPerGoroutine)
		})
	}
}

func BenchmarkEnqueueChFixed(b *testing.B) {
	for _, v := range concurrencyTests {
		concur := v
		b.Run(fmt.Sprintf("concurrency_%d", concur), func(b *testing.B) {
			concurrency, itemsPerGoroutine := splitTotal(b.N, concur)
			enqueueCh(concurrency, itemsPerGoroutine, 10)
		})
	}
}

type payloadY struct {
	fn func(ws *int, p any)
	p  any
}

type payloadX payloadY

//
//type payloadX struct{}

func enqueuePQ(concurrency, itemsPerGoroutine int) {
	total := concurrency * itemsPerGoroutine

	wg := &sync.WaitGroup{}
	wg.Add(1)
	q := NewQueueWithStat[payloadX](0)
	go func() {
		counter := 0
		for {
			q.Reader.Dequeue()
			counter++

			if counter == total {
				wg.Done()
				return
			}
		}
	}()

	for c := 0; c < concurrency; c++ {
		go func() {
			for i := 0; i < itemsPerGoroutine; i++ {
				q.Writer.Enqueue(payloadX{})
			}
		}()
	}

	wg.Wait()
}

func enqueueCh(concurrency, itemsPerGoroutine, chanCapacity int) {
	total := concurrency * itemsPerGoroutine

	wg := &sync.WaitGroup{}
	wg.Add(1)
	ch := make(chan struct{}, chanCapacity)
	go func() {
		counter := 0
		for {
			<-ch
			counter++

			if counter == total {
				wg.Done()
				return
			}
		}
	}()

	for c := 0; c < concurrency; c++ {
		go func() {
			for i := 0; i < itemsPerGoroutine; i++ {
				ch <- struct{}{}
			}
		}()
	}

	wg.Wait()
}
