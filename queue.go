// Copyright (c) 2022, Svetoslav Srebrev <srebrev dot svetoslav at gmail dot com>. All rights reserved.
// Use of this source code is governed by a 3-Clause license that can be found in the LICENSE file.

package pq

import (
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

var ErrCanceled = errors.New("operation canceled")

type Queue[V any] struct {
	Writer *Writer[V]
	Reader *Reader[V]
}

type Writer[V any] struct {
	writeSegment atomic.Pointer[segment[V]]
	mutex        *sync.Mutex
	chReady      chan struct{}
	stat         *pendingCounter
}

type Reader[V any] struct {
	segment   *segment[V]
	readIndex uint64
	chReady   <-chan struct{}
	stat      *pendingCounter
	KeepData  bool
}

type segment[V any] struct {
	slots      [segmentSize]slot[V]
	writeIndex atomic.Uint64
	next       atomic.Pointer[segment[V]]
	id         uint64
	offset     uint64
}

type slot[V any] struct {
	data V
	flag atomic.Uint32
}

type pendingCounter struct {
	batch   int64
	counter atomic.Int64
}

func (pc *pendingCounter) getCount() int {
	if pc == nil {
		return -1
	}

	return int(pc.counter.Load())
}

func NewQueue[V any]() Queue[V] {
	return NewQueueWithStat[V](0)
}

func NewQueueWithStat[V any](batchIncrement int) Queue[V] {
	chReady := make(chan struct{}, 1)
	seg := &segment[V]{}

	var pc *pendingCounter
	if batchIncrement > 0 {
		pc = &pendingCounter{batch: int64(batchIncrement)}
	}

	qw := &Writer[V]{chReady: chReady, mutex: &sync.Mutex{}, stat: pc}
	qw.writeSegment.Store(seg)

	qr := &Reader[V]{chReady: chReady, segment: seg, readIndex: 0, stat: pc}
	return Queue[V]{Writer: qw, Reader: qr}
}

func (qw *Writer[V]) Pending() int {
	return qw.stat.getCount()
}

func (qw *Writer[V]) Enqueue(value V) {
	qw.enqueue(value)
}

func (qw *Writer[V]) EnqueueWithIndex(value V) uint64 {
	seg, index := qw.enqueue(value)
	return seg.offset + index
}

func (qr *Reader[V]) Pending() int {
	return qr.stat.getCount()
}

func (qr *Reader[V]) Dequeue() V {
	_, val, _ := qr.dequeue(true)
	return val
}

func (qr *Reader[V]) DequeueWithIndex() (uint64, V) {
	index, val, _ := qr.dequeue(true)
	return index, val
}

func (qr *Reader[V]) TryDequeue() (uint64, V, bool) {
	return qr.dequeue(false)
}

func DequeueWithCancel[V, C any](qr *Reader[V], chCancel <-chan C) (uint64, V, error) {
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
			case <-chCancel:
				swapped := qr.segment.slots[qr.readIndex].flag.CompareAndSwap(fWaiting, fClear)
				if !swapped {
					// prev == fReady. The item was set concurrently with chCancel.
					// Consume qr.chReady to prevent next item from being read before it is first set.
					<-qr.chReady
				}
				var zero V
				return 0, zero, ErrCanceled

			case <-qr.chReady:
			}
		}
	}

	index := qr.readIndex
	qr.readIndex += 1
	offset := qr.segment.offset + index

	if qr.stat != nil {
		items := offset + 1
		if items%uint64(qr.stat.batch) == 0 {
			qr.stat.counter.Add(-qr.stat.batch)
		}
	}

	data := qr.segment.slots[index].data
	if !qr.KeepData {
		var zero V
		qr.segment.slots[index].data = zero
	}
	return offset, data, nil
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
			var zero V
			return 0, zero, false
		}

		prev := qr.segment.slots[qr.readIndex].flag.Swap(fWaiting)
		if prev != fReady {
			<-qr.chReady
		}
	}

	index := qr.readIndex
	qr.readIndex += 1
	offset := qr.segment.offset + index

	if qr.stat != nil {
		items := offset + 1
		if items%uint64(qr.stat.batch) == 0 {
			qr.stat.counter.Add(-qr.stat.batch)
		}
	}

	data := qr.segment.slots[index].data
	if !qr.KeepData {
		var zero V
		qr.segment.slots[index].data = zero
	}
	return offset, data, true
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

	if qw.stat != nil {
		items := seg.offset + index + 1
		if items%uint64(qw.stat.batch) == 0 {
			qw.stat.counter.Add(qw.stat.batch)
		}
	}

	seg.slots[index].data = value
	prev := seg.slots[index].flag.Swap(fReady)
	if prev == fWaiting {
		qw.chReady <- struct{}{}
	}

	return seg, index
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
