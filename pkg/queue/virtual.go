package queue

import (
	"bytes"
	"sync"

	"github.com/pkg/errors"
)

type virtualQueue struct {
	mutex sync.Mutex
	stack []*virtualSegment
}

func newVirtualQueue() Queue {
	return &virtualQueue{
		sync.Mutex{},
		make([]*virtualSegment, 0),
	}
}

func (q *virtualQueue) Enqueue() (WriteSegment, error) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	s := &virtualSegment{}
	q.stack = append(q.stack, s)
	return s, nil
}

func (q *virtualQueue) Dequeue() (ReadSegment, error) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if len(q.stack) == 0 {
		return nil, errors.New("nothing to dequeue")
	}

	var s ReadSegment
	s, q.stack = q.stack[0], q.stack[1:]

	return s, nil
}

type virtualSegment struct {
	buffer *bytes.Buffer
}

func newVirtualSegment() *virtualSegment {
	return &virtualSegment{new(bytes.Buffer)}
}
func (v *virtualSegment) Read(b []byte) (int, error) {
	return v.buffer.Read(b)
}

func (v *virtualSegment) Write(b []byte) (int, error) {
	return v.buffer.Write(b)
}

func (v *virtualSegment) Sync() error  { return nil }
func (v *virtualSegment) Close() error { return nil }

func (v *virtualSegment) Delete() error {
	v.buffer.Reset()
	return nil
}

func (v *virtualSegment) Commit() error { return nil }
func (v *virtualSegment) Failed() error { return nil }

func (v *virtualSegment) Size() int64 {
	return int64(v.buffer.Len())
}
