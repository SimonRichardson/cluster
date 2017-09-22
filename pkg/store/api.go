package store

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/SimonRichardson/cluster/pkg/members"
	"github.com/SimonRichardson/cluster/pkg/metrics"
	"github.com/SimonRichardson/cluster/pkg/uuid"
	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
)

const (

	// APIPathReplicate represents a way to replicate a segment by id.
	APIPathReplicate = "/replicate"
)

// ClusterPeer models cluster.Peer.
type ClusterPeer interface {
	Current(members.PeerType) ([]string, error)
	State() map[string]interface{}
}

// API serves the store API
type API struct {
	peer               ClusterPeer
	replicatedSegments metrics.Counter
	replicatedBytes    metrics.Counter
	duration           metrics.HistogramVec
	logger             log.Logger
}

// NewAPI returns a usable API.
func NewAPI(
	peer ClusterPeer,
	replicatedSegments, replicatedBytes metrics.Counter,
	duration metrics.HistogramVec,
	logger log.Logger,
) *API {
	return &API{
		peer:               peer,
		replicatedSegments: replicatedSegments,
		replicatedBytes:    replicatedBytes,
		duration:           duration,
		logger:             logger,
	}
}

// Close out the API
func (a *API) Close() error {
	return nil
}

func (a *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	iw := &interceptingWriter{http.StatusOK, w}
	w = iw

	defer func(begin time.Time) {
		a.duration.WithLabelValues(
			r.Method,
			r.URL.Path,
			strconv.Itoa(iw.code),
		).Observe(time.Since(begin).Seconds())
	}(time.Now())

	method, path := r.Method, r.URL.Path
	switch {
	case method == "POST" && path == APIPathReplicate:
		a.handleReplicate(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (a *API) handleReplicate(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	segment, err := a.log.Create()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var buf bytes.Buffer
	n, err := teeRecords(r.Body, segment, &buf)
	if err != nil {
		segment.Delete()
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	} else if n == 0 {
		segment.Delete()
		fmt.Fprintln(w, "No records")
		return
	}
	if err := segment.Close(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	a.replicatedSegments.Inc()
	a.replicatedBytes.Add(float64(n))

	fmt.Fprintln(w, "OK")
}

type interceptingWriter struct {
	code int
	http.ResponseWriter
}

func (iw *interceptingWriter) WriteHeader(code int) {
	iw.code = code
	iw.ResponseWriter.WriteHeader(code)
}

func teeRecords(src io.Reader, dst ...io.Writer) (n int, err error) {
	var (
		w = io.MultiWriter(dst...)
		s = bufio.NewScanner(src)
	)
	s.Split(scanLinesPreserveNewline)
	for s.Scan() {
		record := s.Bytes()
		if len(record) == 0 {
			continue
		}

		fields := bytes.Fields(record)
		if len(fields) == 0 {
			return 0, errInvalidUUID{errors.Errorf("missing uuid")}
		}

		_, err := uuid.ParseBytes(fields[0])
		if err != nil {
			return 0, errInvalidUUID{errors.Errorf("invalid uuid")}
		}

		n0, err := w.Write(record)
		if err != nil {
			return 0, err
		}
		n += n0
	}
	return n, s.Err()
}

type invalidUUID interface {
	InvalidUUID() bool
}

type errInvalidUUID struct {
	err error
}

func (e errInvalidUUID) Error() string {
	return e.err.Error()
}

func (e errInvalidUUID) InvalidUUID() bool {
	return true
}

// ErrInvalidUUID tests to see if the error passed is a not found error or not.
func ErrInvalidUUID(err error) bool {
	if err != nil {
		if _, ok := err.(invalidUUID); ok {
			return true
		}
	}
	return false
}

// Like bufio.ScanLines, but retain the \n.
func scanLinesPreserveNewline(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if i := bytes.IndexByte(data, '\n'); i >= 0 {
		return i + 1, data[0 : i+1], nil
	}
	if atEOF {
		return len(data), data, nil
	}
	return 0, nil, nil
}
