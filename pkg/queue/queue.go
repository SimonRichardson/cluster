package queue

// Queue is an abstraction for segments on an ingest node.
type Queue interface {

	// Enqueue returns a new segment that can be written to.
	Enqueue() (WriteSegment, error)

	// Dequeue returns the first segment, which can then be read from.
	Dequeue() (ReadSegment, error)
}

type noSegmentsAvailable interface {
	NoSegmentsAvailable() bool
}

type errNoSegmentsAvailable struct {
	err error
}

func (e errNoSegmentsAvailable) Error() string {
	return e.err.Error()
}

func (e errNoSegmentsAvailable) NoSegmentsAvailable() bool {
	return true
}

// ErrNoSegmentsAvailable tests to see if the error passed if no segments are
// available
func ErrNoSegmentsAvailable(err error) bool {
	if err != nil {
		if _, ok := err.(noSegmentsAvailable); ok {
			return true
		}
	}
	return false
}
