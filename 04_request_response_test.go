package pq

import (
	"fmt"
	"sync"
	"testing"
)

func BenchmarkReqRespQCh(b *testing.B) {
	for _, v := range concurrencyTests {
		concur := v
		b.Run(fmt.Sprintf("concurrency_%d", concur), func(b *testing.B) {
			concurrency, itemsPerGoroutine := splitTotal(b.N, concur)
			reqRespQCh(concurrency, itemsPerGoroutine)
		})
	}
}

func BenchmarkReqRespCh(b *testing.B) {
	for _, v := range concurrencyTests {
		concur := v
		b.Run(fmt.Sprintf("concurrency_%d", concur), func(b *testing.B) {
			concurrency, itemsPerGoroutine := splitTotal(b.N, concur)
			reqRespCh(concurrency, itemsPerGoroutine)
		})
	}
}

func BenchmarkReqRespQQ(b *testing.B) {
	for _, v := range concurrencyTests {
		concur := v
		b.Run(fmt.Sprintf("concurrency_%d", concur), func(b *testing.B) {
			concurrency, itemsPerGoroutine := splitTotal(b.N, concur)
			reqRespQQ(concurrency, itemsPerGoroutine)
		})
	}
}

func reqRespQCh(concurrency, itemsPerGoroutine int) {
	reqQW, reqQR := NewQueue[chan struct{}]()
	go func() {
		for {
			complete := reqQR.Dequeue()
			complete <- struct{}{}
		}
	}()

	wg := &sync.WaitGroup{}
	for c := 0; c < concurrency; c++ {
		wg.Add(1)
		go func() {
			chComplete := make(chan struct{}, 1)
			for i := 0; i < itemsPerGoroutine; i++ {
				reqQW.Enqueue(chComplete)
				<-chComplete
			}
			wg.Done()
		}()
	}

	wg.Wait()
}

func reqRespCh(concurrency, itemsPerGoroutine int) {
	chRecResp := make(chan chan struct{}, concurrency)
	go func() {
		for {
			complete := <-chRecResp
			complete <- struct{}{}
		}
	}()

	wg := &sync.WaitGroup{}
	for c := 0; c < concurrency; c++ {
		wg.Add(1)
		go func() {
			chComplete := make(chan struct{}, 1)
			for i := 0; i < itemsPerGoroutine; i++ {
				chRecResp <- chComplete
				<-chComplete
			}
			wg.Done()
		}()
	}

	wg.Wait()
}

func reqRespQQ(concurrency, itemsPerGoroutine int) {
	reqQW, reqQR := NewQueue[*Writer[struct{}]]()
	go func() {
		for {
			completeQW := reqQR.Dequeue()
			completeQW.Enqueue(struct{}{})
		}
	}()

	wg := &sync.WaitGroup{}
	for c := 0; c < concurrency; c++ {
		wg.Add(1)
		go func() {
			completeQW, completeQR := NewQueue[struct{}]()
			for i := 0; i < itemsPerGoroutine; i++ {
				reqQW.Enqueue(completeQW)
				completeQR.Dequeue()
			}
			wg.Done()
		}()
	}

	wg.Wait()
}
