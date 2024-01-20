// Copyright (c) 2022, Svetoslav Srebrev <srebrev dot svetoslav at gmail dot com>. All rights reserved.
// Use of this source code is governed by a 3-Clause license that can be found in the LICENSE file.
package pq

import (
	"sync"
	"sync/atomic"
)

const (
	segmentSizeS = 16
)

type QueueS[V any] struct {
	Writer *WriterS[V]
	Reader *ReaderS[V]
}

type WriterS[V any] struct {
	writeSegment  atomic.Pointer[segmentS[V]]
	headSegment   *segmentS[V]
	readSegmentId atomic.Uint64
	mutex         *sync.Mutex
	chReady       chan struct{}
	stat          *pendingCounter
	_pad          [1]byte
}

type ReaderS[V any] struct {
	segment   *segmentS[V]
	readIndex uint64
	writer    *WriterS[V]
	chReady   <-chan struct{}
	stat      *pendingCounter
	KeepData  bool
}

type ReadRepeaterS[V any] struct {
	reader    *ReaderS[V]
	segment   *segmentS[V]
	readIndex uint64
	KeepData  bool
}

type segmentS[V any] struct {
	slots      [segmentSizeS]slot[V]
	writeIndex atomic.Uint64
	next       atomic.Pointer[segmentS[V]]
	id         uint64
	offset     uint64
}

func NewQueueS[V any]() QueueS[V] {
	return NewQueueWithStatS[V](0)
}

func NewQueueWithStatS[V any](batchIncrement int) QueueS[V] {
	chReady := make(chan struct{}, 1)
	seg := &segmentS[V]{}

	var pc *pendingCounter
	if batchIncrement > 0 {
		pc = &pendingCounter{batch: int64(batchIncrement)}
	}

	qw := &WriterS[V]{chReady: chReady, mutex: &sync.Mutex{}, stat: pc}
	qw.headSegment = seg
	qw.writeSegment.Store(seg)

	qr := &ReaderS[V]{chReady: chReady, segment: seg, readIndex: 0, writer: qw, stat: pc}
	return QueueS[V]{Writer: qw, Reader: qr}
}

func (qw *WriterS[V]) Pending() int {
	return qw.stat.getCount()
}

func (qw *WriterS[V]) Enqueue(value V) {
	qw.enqueue(value)
}

func (qw *WriterS[V]) EnqueueWithIndex(value V) uint64 {
	seg, index := qw.enqueue(value)
	return seg.offset + index
}

func (qr *ReaderS[V]) Pending() int {
	return qr.stat.getCount()
}

func (qr *ReaderS[V]) Dequeue() V {
	_, val, _ := qr.dequeue(true)
	return val
}

func (qr *ReaderS[V]) DequeueWithIndex() (uint64, V) {
	index, val, _ := qr.dequeue(true)
	return index, val
}

func (qr *ReaderS[V]) TryDequeue() (uint64, V, bool) {
	return qr.dequeue(false)
}

func DequeueWithCancelS[V, C any](qr *ReaderS[V], chCancel <-chan C) (uint64, V, error) {
	if qr.readIndex == segmentSizeS {
		// qr.segment.next is not nil at this point because previous segment slot seg.slots[segmentSize-1].flag
		// was set which happens AFTER seg.next is set. See enqueue method
		next := qr.segment.next.Load()
		qr.segment = next
		qr.readIndex = 0
		qr.writer.setReadSegmentId(next.id)
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

	qr.segment.slots[qr.readIndex].flag.Store(fClear)

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

func (qr *ReaderS[V]) KeepDataAndCreateRepeater() *ReadRepeaterS[V] {
	qr.KeepData = true
	return &ReadRepeaterS[V]{reader: qr, segment: qr.segment, readIndex: qr.readIndex}
}

func (r *ReadRepeaterS[V]) NextAvailable() (uint64, V, bool) {
	dataAvailable := r.segment.offset+r.readIndex < r.reader.segment.offset+r.reader.readIndex
	if !dataAvailable {
		var zero V
		return 0, zero, false
	}

	if r.readIndex == segmentSizeS {
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

func (qr *ReaderS[V]) dequeue(block bool) (uint64, V, bool) {
	if qr.readIndex == segmentSizeS {
		// qr.segment.next is not nil at this point because previous segment slot seg.slots[segmentSize-1].flag
		// was set which happens AFTER seg.next is set. See enqueue method
		next := qr.segment.next.Load()
		qr.segment = next
		qr.readIndex = 0
		qr.writer.setReadSegmentId(next.id)
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

	qr.segment.slots[qr.readIndex].flag.Store(fClear)

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

func (qw *WriterS[V]) enqueue(value V) (*segmentS[V], uint64) {
	seg := qw.writeSegment.Load()
	var index uint64
	for {
		index = seg.writeIndex.Add(1) - 1
		if index < segmentSizeS {
			if index == segmentSizeS-1 {

				qw.setNextSegment(seg)
				//qw.setNextSegmentLockFree(seg)
			}
			break
		} else {
			//if seg.writeIndex.Load() < segmentSize {
			//	index = seg.writeIndex.Add(1) - 1
			//	if index < segmentSize {
			//		break
			//	}
			//
			//}
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

func (qw *WriterS[V]) setNextSegment(seg *segmentS[V]) *segmentS[V] {
	qw.mutex.Lock()
	next := seg.next.Load()
	if next == nil {
		if seg.writeIndex.Load() < segmentSizeS {
			qw.mutex.Unlock()
			return seg
		}

		headSeg := qw.headSegment
		if headSeg.id < qw.readSegmentId.Load() {
			qw.headSegment = headSeg.next.Load()

			segId := seg.id + 1
			headSeg.id = segId
			headSeg.offset = segId * segmentSizeS
			headSeg.next.Store(nil)
			headSeg.writeIndex.Store(0)

			next = headSeg
		} else {
			next = &segmentS[V]{}
			segId := seg.id + 1
			next.id = segId
			next.offset = segId * segmentSizeS
		}

		seg.next.Store(next)
		qw.writeSegment.Store(next)

	} else {
		next = qw.writeSegment.Load()
	}
	qw.mutex.Unlock()

	return next
}

func (qw *WriterS[V]) setReadSegmentId(id uint64) {
	qw.readSegmentId.Store(id)
}
