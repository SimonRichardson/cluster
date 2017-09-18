package ingester

// Queue is an abstraction for segments on an ingest node.
type Queue interface {

	// Enqueue returns a new segment that can be written to.
	Enqueue() (WriteSegment, error)

	// Dequeue returns the first segment, which can then be read from.
	Dequeue() (ReadSegment, error)
}
