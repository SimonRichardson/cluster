package queue

type nopQueue struct{}

func newNopQueue() Queue {
	return nopQueue{}
}

func (q nopQueue) Enqueue() (WriteSegment, error) { return nopSegment{}, nil }
func (q nopQueue) Dequeue() (ReadSegment, error)  { return nopSegment{}, nil }

type nopSegment struct{}

func (v nopSegment) Read(b []byte) (int, error)  { return 0, nil }
func (v nopSegment) Write(b []byte) (int, error) { return 0, nil }
func (v nopSegment) Sync() error                 { return nil }
func (v nopSegment) Close() error                { return nil }
func (v nopSegment) Delete() error               { return nil }
func (v nopSegment) Commit() error               { return nil }
func (v nopSegment) Failed() error               { return nil }
func (v nopSegment) Size() int64                 { return 0 }
