package clients

import (
	"io"
)

// Client defines an abstraction for requesting information from some source,
// either http, tcp, etc...
type Client interface {

	// Get sends a request and returns a valid response or an error if the request
	// was a failure
	Get(string) (Response, error)

	// Post sends a request with a body and returns a response or an error if the
	// request was a failure
	Post(string, []byte) (Response, error)
}

// Response defines a interface for retrieving the data from a request to the
// source
type Response interface {

	// Status returns the request status code
	Status() int

	// Bytes returns a series of bytes if successful or an error if a failure
	// occurred
	Bytes() ([]byte, error)

	// Reader returns the body as a reader closer
	Reader() io.ReadCloser

	// Close the response
	Close() error
}
