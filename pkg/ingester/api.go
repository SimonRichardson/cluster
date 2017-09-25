package ingester

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/SimonRichardson/cluster/pkg/metrics"
	"github.com/SimonRichardson/cluster/pkg/uuid"
)

const (

	// APIPathNext represents what the next segment to work on
	APIPathNext = "/next"

	// APIPathRead represents a way to read the segment by id
	APIPathRead = "/read"

	// APIPathCommit represents a way to commit a segment by id, so that it's no
	// longer offered for reading.
	APIPathCommit = "/commit"

	// APIPathFailed represents a way to fail a segment by id.
	APIPathFailed = "/failed"
)

// API serves the ingest API.
type API struct {
	queue                             Queue
	timeout                           time.Duration
	pending                           map[string]pendingSegment
	action                            chan func()
	stop                              chan chan struct{}
	clients                           metrics.Gauge
	failedSegments                    metrics.Counter
	committedSegments, committedBytes metrics.Counter
	duration                          metrics.HistogramVec
}

type pendingSegment struct {
	segment  ReadSegment
	deadline time.Time
	reading  bool
}

// NewAPI returns a usable ingest API.
func NewAPI(
	queue Queue,
	pendingSegmentTimeout time.Duration,
	clients metrics.Gauge,
	failedSegments, committedSegments, committedBytes metrics.Counter,
	duration metrics.HistogramVec,
) *API {
	a := &API{
		timeout:           pendingSegmentTimeout,
		pending:           map[string]pendingSegment{},
		action:            make(chan func()),
		stop:              make(chan chan struct{}),
		clients:           clients,
		failedSegments:    failedSegments,
		committedSegments: committedSegments,
		committedBytes:    committedBytes,
		duration:          duration,
	}
	go a.loop()
	return a
}

func (a *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	iw := &interceptingWriter{http.StatusOK, w}
	w = iw

	// Metrics
	a.clients.Inc()
	defer a.clients.Dec()

	defer func(begin time.Time) {
		a.duration.WithLabelValues(
			r.Method,
			r.URL.Path,
			strconv.Itoa(iw.code),
		).Observe(time.Since(begin).Seconds())
	}(time.Now())

	// Routing table
	method, path := r.Method, r.URL.Path
	switch {
	case method == "GET" && path == APIPathNext:
		a.handleNext(w, r)
	case method == "GET" && path == APIPathRead:
		a.handleRead(w, r)
	case method == "POST" && path == APIPathCommit:
		a.handleCommit(w, r)
	case method == "POST" && path == APIPathFailed:
		a.handleFailed(w, r)
	default:
		// Nothing found
		http.NotFound(w, r)
	}
}

// Stop terminates the API.
func (a *API) Stop() {
	c := make(chan struct{})
	a.stop <- c
	<-c
}

func (a *API) loop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case f := <-a.action:
			f()

		case now := <-ticker.C:
			a.clean(now)

		case c := <-a.stop:
			// fail all pending segments
			a.clean(time.Now().Add(10 * a.timeout))
			close(c)
			return
		}
	}
}

// Fail any pending segment past its deadline,
// making it available for consumption again.
func (a *API) clean(now time.Time) {
	for id, s := range a.pending {

		if now.After(s.deadline) {
			if err := s.segment.Failed(); err != nil {
				panic(err)
			}
			delete(a.pending, id)

			a.failedSegments.Inc()
		}
	}
}

func (a *API) handleNext(w http.ResponseWriter, r *http.Request) {
	var (
		notFoundError       = make(chan struct{})
		internalServerError = make(chan error)
		nextID              = make(chan string)
	)
	a.action <- func() {
		s, err := a.queue.Dequeue()
		if ErrNotFound(err) {
			close(notFoundError)
			return
		}
		if err != nil {
			internalServerError <- err
			return
		}
		id, err := uuid.New()
		if err != nil {
			internalServerError <- err
			return
		}

		a.pending[id.String()] = pendingSegment{s, time.Now().Add(a.timeout), false}
		nextID <- id.String()
	}
	select {
	case <-notFoundError:
		http.NotFound(w, r)

	case err := <-internalServerError:
		http.Error(w, err.Error(), http.StatusInternalServerError)

	case id := <-nextID:
		fmt.Fprint(w, id)
	}
}

func (a *API) handleRead(w http.ResponseWriter, r *http.Request) {
	var (
		segment         = make(chan ReadSegment)
		notFoundError   = make(chan struct{})
		concurrentError = make(chan struct{})
	)
	a.action <- func() {
		id := r.URL.Query().Get("id")
		s, ok := a.pending[id]
		if !ok {
			close(notFoundError)
			return
		}
		if s.reading {
			close(concurrentError)
			return
		}
		s.reading = true
		a.pending[id] = s
		segment <- s.segment
	}
	select {
	case s := <-segment:
		io.Copy(w, s)

	case <-notFoundError:
		http.NotFound(w, r)

	case <-concurrentError:
		http.Error(w, "another client is already reading this segment", http.StatusInternalServerError)
	}
}

func (a *API) handleCommit(w http.ResponseWriter, r *http.Request) {
	var (
		notFoundError = make(chan struct{})
		notReadError  = make(chan struct{})
		commitError   = make(chan error)
		commitOK      = make(chan int64)
	)
	a.action <- func() {
		id := r.URL.Query().Get("id")
		s, ok := a.pending[id]
		if !ok {
			close(notFoundError)
			return
		}
		if !s.reading {
			close(notReadError)
			return
		}
		sz := s.segment.Size()
		if err := s.segment.Commit(); err != nil {
			commitError <- err
			return
		}
		delete(a.pending, id)
		commitOK <- sz
	}

	select {
	case <-notFoundError:
		http.NotFound(w, r)

	case <-notReadError:
		http.Error(w, "segment hasn't been read yet; can't commit", http.StatusPreconditionRequired)

	case err := <-commitError:
		http.Error(w, err.Error(), http.StatusInternalServerError)

	case n := <-commitOK:
		a.committedSegments.Inc()
		a.committedBytes.Add(float64(n))

		fmt.Fprint(w, "Commit OK")
	}
}

// Failed marks a pending segment failed.
func (a *API) handleFailed(w http.ResponseWriter, r *http.Request) {
	var (
		notFoundError = make(chan struct{})
		failedError   = make(chan error)
		failedOK      = make(chan struct{})
	)

	a.action <- func() {
		id := r.URL.Query().Get("id")
		s, ok := a.pending[id]
		if !ok {
			close(notFoundError)
			return
		}

		if err := s.segment.Failed(); err != nil {
			failedError <- err
			return
		}

		delete(a.pending, id)
		close(failedOK)
	}

	select {
	case <-notFoundError:
		http.NotFound(w, r)

	case err := <-failedError:
		http.Error(w, err.Error(), http.StatusInternalServerError)

	case <-failedOK:
		a.failedSegments.Inc()

		fmt.Fprint(w, "Failed OK")
	}
}

type interceptingWriter struct {
	code int
	http.ResponseWriter
}

func (iw *interceptingWriter) WriteHeader(code int) {
	iw.code = code
	iw.ResponseWriter.WriteHeader(code)
}
