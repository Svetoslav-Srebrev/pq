// BSD 3-Clause License
//
// Copyright (c) 2022, Svetoslav Srebrev <srebrev dot svetoslav at gmail dot com>
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// 1. Redistributions of source code must retain the above copyright notice, this
// list of conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright notice,
// this list of conditions and the following disclaimer in the documentation
// and/or other materials provided with the distribution.
//
// 3. Neither the name of the copyright holder nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package pq

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

const (
	segmentSize = 331

	fClear   = uint32(0)
	fWaiting = uint32(1)
	fReady   = uint32(2)
)

type slot[V any] struct {
	data V
	flag atomic.Uint32
}

type segment[V any] struct {
	slots      [segmentSize]slot[V]
	writeIndex atomic.Uint64
	next       atomic.Pointer[segment[V]]
	id         uint64
	offset     uint64
}

type Writer[V any] struct {
	writeSegment atomic.Pointer[segment[V]]
	mutex        *sync.Mutex
	chReady      chan struct{}
}

type Reader[V any] struct {
	segment   *segment[V]
	readIndex uint64
	chReady   <-chan struct{}
	_padding  uint64
}

func NewQueue[V any]() (*Writer[V], *Reader[V]) {
	chReady := make(chan struct{}, 1)
	seg := &segment[V]{}

	qw := &Writer[V]{chReady: chReady, mutex: &sync.Mutex{}}
	qw.writeSegment.Store(seg)

	qr := &Reader[V]{chReady: chReady, segment: seg, readIndex: 0}
	return qw, qr
}

func (qw *Writer[V]) Enqueue(value V) {
	qw.enqueue(value)
}

func (qw *Writer[V]) EnqueueWithIndex(value V) uint64 {
	seg, index := qw.enqueue(value)
	return seg.offset + index
}

func (qr *Reader[V]) Dequeue() V {
	_, val, _ := qr.dequeue(true)
	return val
}

func (qr *Reader[V]) DequeueWithIndex() (uint64, V) {
	index, val, _ := qr.dequeue(true)
	return index, val
}

func (qr *Reader[V]) TryDequeue() (V, bool) {
	_, val, success := qr.dequeue(false)
	return val, success
}

func (qr *Reader[V]) TryDequeueWithIndex() (uint64, V, bool) {
	return qr.dequeue(false)
}

func (qr *Reader[V]) DequeueCtx(ctx context.Context) (V, error) {
	_, val, err := qr.dequeueCtx(ctx)
	return val, err
}

func (qr *Reader[V]) DequeueCtxWithIndex(ctx context.Context) (uint64, V, error) {
	return qr.dequeueCtx(ctx)
}

func (qw *Writer[V]) enqueue(value V) (*segment[V], uint64) {
	seg := qw.writeSegment.Load()
	var index uint64
	for {
		index = seg.writeIndex.Add(1) - 1
		if index < segmentSize {
			if index == segmentSize-1 {

				qw.setNextSegment(seg)
				//qw.setNextSegmentLockFree(seg)
			}
			break
		} else {

			seg = qw.setNextSegment(seg)
			//seg = qw.setNextSegmentLockFree(seg)
		}
	}

	seg.slots[index].data = value
	prev := seg.slots[index].flag.Swap(fReady)
	if prev == fWaiting {
		qw.chReady <- struct{}{}
	}
	return seg, index
}

func (qr *Reader[V]) dequeue(block bool) (uint64, V, bool) {
	if qr.readIndex == segmentSize {
		// qr.segment.next is not nil at this point because previous segment slot seg.slots[segmentSize-1].flag
		// was set which happens AFTER seg.next is set. See enqueue method
		next := qr.segment.next.Load()
		qr.segment = next
		qr.readIndex = 0
	}

	// index < segmentSize
	if qr.segment.slots[qr.readIndex].flag.Load() != fReady {
		if !block {
			var zeroValue V
			return 0, zeroValue, false
		}

		prev := qr.segment.slots[qr.readIndex].flag.Swap(fWaiting)
		if prev != fReady {
			<-qr.chReady
		}
	}

	index := qr.readIndex
	qr.readIndex += 1
	return qr.segment.offset + index, qr.segment.slots[index].data, true
}

func (qr *Reader[V]) dequeueCtx(ctx context.Context) (uint64, V, error) {
	if qr.readIndex == segmentSize {
		// qr.segment.next is not nil at this point because previous segment slot seg.slots[segmentSize-1].flag
		// was set which happens AFTER seg.next is set. See enqueue method
		next := qr.segment.next.Load()
		qr.segment = next
		qr.readIndex = 0
	}

	// index < segmentSize
	if qr.segment.slots[qr.readIndex].flag.Load() != fReady {

		prev := qr.segment.slots[qr.readIndex].flag.Swap(fWaiting)
		if prev != fReady {
			select {
			case <-ctx.Done():
				prev = qr.segment.slots[qr.readIndex].flag.Swap(fClear)
				if prev != fReady {
					err := ctx.Err()
					if err == nil {
						// ctx.Done() is closed/signalled but, but ctx.Err() is nil? context.Context interface custom implementation bug?
						err = errors.New("operation canceled")
					}

					var zeroValue V
					return 0, zeroValue, err
				}
				// prev == fReady. The item was set concurrently with ctx.Done() channel.
				// Consume qr.chReady and pretend we noticed qr.chReady first
				<-qr.chReady
			case <-qr.chReady:
			}
		}
	}

	index := qr.readIndex
	qr.readIndex += 1
	return qr.segment.offset + index, qr.segment.slots[index].data, nil
}

func (qw *Writer[V]) setNextSegment(seg *segment[V]) *segment[V] {
	qw.mutex.Lock()
	next := seg.next.Load()
	if next == nil {
		next = &segment[V]{}
		next.id = seg.id + 1
		next.offset = next.id * segmentSize
		seg.next.Store(next)
		qw.writeSegment.Store(next)

	} else {
		next = qw.writeSegment.Load()
	}
	qw.mutex.Unlock()

	return next
}

func (qw *Writer[V]) setNextSegmentLockFree(seg *segment[V]) *segment[V] {
	next := seg.next.Load()
	if next == nil {
		next = &segment[V]{}
		next.id = seg.id + 1
		next.offset = next.id * segmentSize

		if seg.next.CompareAndSwap(nil, next) {
			qw.writeSegment.CompareAndSwap(seg, next)
			return next
		}
	}

	return qw.writeSegment.Load()
}
