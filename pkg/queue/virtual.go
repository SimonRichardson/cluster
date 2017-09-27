package queue

import (
	"bytes"
	"io"
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

	s := newVirtualSegment()
	q.stack = append(q.stack, s)
	return s, nil
}

func (q *virtualQueue) Dequeue() (ReadSegment, error) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if len(q.stack) == 0 {
		return nil, errNoSegmentsAvailable{errors.New("nothing found for reading")}
	}

	var s ReadSegment
	s, q.stack = q.stack[0], q.stack[1:]

	return s, nil
}

type virtualSegment struct {
	buffer *bytes.Buffer
	size   *countingWriter
}

func newVirtualSegment() *virtualSegment {
	return &virtualSegment{
		new(bytes.Buffer),
		&countingWriter{},
	}
}
func (v *virtualSegment) Read(b []byte) (int, error) {
	return v.buffer.Read(b)
}

func (v *virtualSegment) Write(b []byte) (int, error) {
	w := io.MultiWriter(v.buffer, v.size)
	return w.Write(b)
}

func (v *virtualSegment) Sync() error  { return nil }
func (v *virtualSegment) Close() error { return nil }

func (v *virtualSegment) Delete() error {
	v.buffer.Reset()
	v.size.Reset()
	return nil
}

func (v *virtualSegment) Commit() error { return nil }
func (v *virtualSegment) Failed() error { return nil }

func (v *virtualSegment) Size() int64 {
	return v.size.Len()
}

type countingWriter struct {
	size int64
}

func (c *countingWriter) Write(b []byte) (n int, err error) {
	n = len(b)
	c.size += int64(n)
	return
}

func (c *countingWriter) Reset()     { c.size = 0 }
func (c *countingWriter) Len() int64 { return c.size }
