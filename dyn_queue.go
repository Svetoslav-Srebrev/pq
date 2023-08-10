// Copyright (c) 2022, Svetoslav Srebrev <srebrev dot svetoslav at gmail dot com>. All rights reserved.
// Use of this source code is governed by a 3-Clause license that can be found in the LICENSE file.

package pq

import (
	"sync"
	"sync/atomic"
)

const (
	defaultDynSegmentSize = 13
)

type DynQueue[V any] struct {
	Writer *DynWriter[V]
	Reader *DynReader[V]
}

type DynWriter[V any] struct {
	writeSegment atomic.Pointer[dynSegment[V]]
	mutex        *sync.Mutex
	chReady      chan struct{}
	stat         *pendingCounter
}

type DynReader[V any] struct {
	segment   *dynSegment[V]
	readIndex uint64
	chReady   <-chan struct{}
	stat      *pendingCounter
	KeepData  bool
}

type DynReadRepeater[V any] struct {
	reader    *DynReader[V]
	segment   *dynSegment[V]
	readIndex uint64
	KeepData  bool
}

type dynSegment[V any] struct {
	slots      []slot[V]
	writeIndex atomic.Uint64
	next       atomic.Pointer[dynSegment[V]]
	id         uint64
	offset     uint64
}

func NewDynQueue[V any]() DynQueue[V] {
	return NewDynQueueWithStat[V](defaultDynSegmentSize, 0)
}

func NewDynQueueSized[V any](numSlots int) DynQueue[V] {
	return NewDynQueueWithStat[V](numSlots, 0)
}

func NewDynQueueWithStat[V any](numSlots int, batchIncrement int) DynQueue[V] {
	chReady := make(chan struct{}, 1)
	seg := &dynSegment[V]{}
	seg.slots = make([]slot[V], numSlots)

	var pc *pendingCounter
	if batchIncrement > 0 {
		pc = &pendingCounter{batch: int64(batchIncrement)}
	}

	qw := &DynWriter[V]{chReady: chReady, mutex: &sync.Mutex{}, stat: pc}
	qw.writeSegment.Store(seg)

	qr := &DynReader[V]{chReady: chReady, segment: seg, readIndex: 0, stat: pc}
	return DynQueue[V]{qw, qr}
}

func (qw *DynWriter[V]) Pending() int {
	return qw.stat.getCount()
}

func (qw *DynWriter[V]) Enqueue(value V) {
	qw.enqueue(value)
}

func (qw *DynWriter[V]) EnqueueWithIndex(value V) uint64 {
	seg, index := qw.enqueue(value)
	return seg.offset + index
}

func (qr *DynReader[V]) Pending() int {
	return qr.stat.getCount()
}

func (qr *DynReader[V]) Dequeue() V {
	_, val, _ := qr.dequeue(true)
	return val
}

func (qr *DynReader[V]) DequeueWithIndex() (uint64, V) {
	index, val, _ := qr.dequeue(true)
	return index, val
}

func (qr *DynReader[V]) TryDequeue() (uint64, V, bool) {
	return qr.dequeue(false)
}

func DynDequeueWithCancel[V, C any](qr *DynReader[V], chCancel <-chan C) (uint64, V, error) {
	if qr.readIndex == uint64(len(qr.segment.slots)) {
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

func (qr *DynReader[V]) KeepDataAndCreateRepeater() *DynReadRepeater[V] {
	qr.KeepData = true
	return &DynReadRepeater[V]{reader: qr, segment: qr.segment, readIndex: qr.readIndex}
}

func (r *DynReadRepeater[V]) NextAvailable() (uint64, V, bool) {
	dataAvailable := r.segment.offset < r.reader.segment.offset || r.readIndex < r.reader.readIndex
	if !dataAvailable {
		var zero V
		return 0, zero, false
	}

	if r.readIndex == uint64(len(r.segment.slots)) {
		next := r.segment.next.Load()
		r.segment = next
		r.readIndex = 0
	}

	index := r.readIndex
	r.readIndex += 1
	offset := r.segment.offset + index

	data := r.segment.slots[index].data
	if !r.KeepData {
		var zero V
		r.segment.slots[index].data = zero
	}
	return offset, data, true
}

func (qr *DynReader[V]) dequeue(block bool) (uint64, V, bool) {
	if qr.readIndex == uint64(len(qr.segment.slots)) {
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

func (qw *DynWriter[V]) enqueue(value V) (*dynSegment[V], uint64) {
	seg := qw.writeSegment.Load()
	var index uint64
	for {
		index = seg.writeIndex.Add(1) - 1
		segSize := uint64(len(seg.slots))
		if index < segSize {
			if index == segSize-1 {

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

func (qw *DynWriter[V]) setNextSegment(seg *dynSegment[V]) *dynSegment[V] {
	qw.mutex.Lock()
	next := seg.next.Load()
	if next == nil {
		next = &dynSegment[V]{}
		next.slots = make([]slot[V], len(seg.slots))
		next.id = seg.id + 1
		next.offset = next.id * uint64(len(seg.slots))
		seg.next.Store(next)
		qw.writeSegment.Store(next)

	} else {
		next = qw.writeSegment.Load()
	}
	qw.mutex.Unlock()

	return next
}
