package pq

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

const pipelineSteps = 3

type resource interface {
	do()
}

type unprotectedResource struct {
	x int64
}

func (r *unprotectedResource) do() {
	r.x++
}

type atomicResource struct {
	x int64
}

func (r *atomicResource) do() {
	atomic.AddInt64(&r.x, 1)
}

type mutexResource struct {
	mu *sync.Mutex
	x  int64
}

func (r *mutexResource) do() {
	r.mu.Lock()
	r.x++
	r.mu.Unlock()
}

func BenchmarkPipelinePQ(b *testing.B) {
	var actions []resource
	for i := 0; i < pipelineSteps; i++ {
		actions = append(actions, &unprotectedResource{})
	}

	for _, v := range concurrencyTests {
		concur := v
		b.Run(fmt.Sprintf("concurrency_%d", concur), func(b *testing.B) {
			concurrency, itemsPerGoroutine := splitTotal(b.N, concur)
			pipelinePQ(concurrency, itemsPerGoroutine, actions)
		})
	}
}

func BenchmarkPipelineCh(b *testing.B) {
	var actions []resource
	for i := 0; i < pipelineSteps; i++ {
		actions = append(actions, &unprotectedResource{})
	}

	for _, v := range concurrencyTests {
		concur := v
		b.Run(fmt.Sprintf("concurrency_%d", concur), func(b *testing.B) {
			concurrency, itemsPerGoroutine := splitTotal(b.N, concur)
			pipelineCh(concurrency, itemsPerGoroutine, actions)
		})
	}
}

func BenchmarkPipelineAtomic(b *testing.B) {
	var actions []resource
	for i := 0; i < pipelineSteps; i++ {
		actions = append(actions, &atomicResource{})
	}

	for _, v := range concurrencyTests {
		concur := v
		b.Run(fmt.Sprintf("concurrency_%d", concur), func(b *testing.B) {
			concurrency, itemsPerGoroutine := splitTotal(b.N, concur)
			pipelineConcurrent(concurrency, itemsPerGoroutine, actions)
		})
	}
}

func BenchmarkPipelineMutex(b *testing.B) {
	var actions []resource
	for i := 0; i < pipelineSteps; i++ {
		actions = append(actions, &mutexResource{mu: &sync.Mutex{}})
	}

	for _, v := range concurrencyTests {
		concur := v
		b.Run(fmt.Sprintf("concurrency_%d", concur), func(b *testing.B) {
			concurrency, itemsPerGoroutine := splitTotal(b.N, concur)
			pipelineConcurrent(concurrency, itemsPerGoroutine, actions)
		})
	}
}

func pipelineConcurrent(concurrency, itemsPerGoroutine int, resources []resource) {
	wg := &sync.WaitGroup{}
	for c := 0; c < concurrency; c++ {
		wg.Add(1)
		go func() {
			for i := 0; i < itemsPerGoroutine; i++ {
				for _, res := range resources {
					res.do()
				}
			}
			wg.Done()
		}()
	}

	wg.Wait()
}

func pipelinePQ(concurrency, itemsPerGoroutine int, resources []resource) {
	total := concurrency * itemsPerGoroutine

	wg := &sync.WaitGroup{}
	wg.Add(1)
	finalQW, finalQR := NewQueue[struct{}]()
	go func() {
		counter := 0
		for {
			finalQR.Dequeue()

			resources[0].do()

			counter++
			if counter == total {
				wg.Done()
				return
			}
		}
	}()

	nextQW := finalQW
	for i := 0; i < len(resources)-1; i++ {
		curQW, curQR := NewQueue[struct{}]()
		go func(resourceIndex int, nqw *Writer[struct{}]) {
			for {
				item := curQR.Dequeue()

				resources[resourceIndex].do()

				nqw.Enqueue(item)
			}
		}(i+1, nextQW)
		nextQW = curQW
	}

	for c := 0; c < concurrency; c++ {
		go func() {
			for i := 0; i < itemsPerGoroutine; i++ {
				nextQW.Enqueue(struct{}{})
			}
		}()
	}

	wg.Wait()
}

func pipelineCh(concurrency, itemsPerGoroutine int, resources []resource) {
	total := concurrency * itemsPerGoroutine
	chanCapacity := total

	wg := &sync.WaitGroup{}
	wg.Add(1)
	finalCh := make(chan struct{}, chanCapacity)
	go func() {
		counter := 0
		for {
			<-finalCh

			resources[0].do()

			counter++
			if counter == total {
				wg.Done()
				return
			}
		}
	}()

	nextCh := finalCh
	for i := 0; i < len(resources)-1; i++ {
		curCh := make(chan struct{}, chanCapacity)
		go func(resourceIndex int, nch chan struct{}) {
			for {
				item := <-curCh

				resources[resourceIndex].do()

				nch <- item
			}
		}(i+1, nextCh)
		nextCh = curCh
	}

	for c := 0; c < concurrency; c++ {
		go func() {
			for i := 0; i < itemsPerGoroutine; i++ {
				nextCh <- struct{}{}
			}
		}()
	}

	wg.Wait()
}
