package queue

import "io"

// WriteSegment is a segment that can be written to. It may be optionally synced
// for persistence manually. When writing is complete, it may be closed and
// flushed.
type WriteSegment interface {
	io.Writer

	// Sync the data for persistence or fails with an error
	Sync() error

	// Close the writer or fails with an error
	Close() error

	// Delete the written segment or fails with an error
	Delete() error
}

// ReadSegment is a segment that can be read from. Once read, it may be
// committed and thus deleted. Or it may be failed, and made available for
// selection again.
type ReadSegment interface {
	io.Reader

	// Commit attempts to to commit a read segment or fails on error
	Commit() error

	// Failed notifies the read segment or fails with an error
	Failed() error

	// Size gets the size of the read segment.
	Size() int64
}
