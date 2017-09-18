package consumer

import (
	"bufio"
	"bytes"
	"io"

	"github.com/SimonRichardson/cluster/pkg/uuid"
	"github.com/pkg/errors"
)

func mergeRecords(w io.Writer, readers ...io.Reader) (n int64, err error) {
	if len(readers) == 0 {
		return 0, nil
	}

	// https://github.com/golang/go/wiki/SliceTricks
	notnil := readers[:0]
	for _, r := range readers {
		if r != nil {
			notnil = append(notnil, r)
		}
	}
	readers = notnil

	// Initialize our state.
	var (
		scanner = make([]*bufio.Scanner, len(readers))
		records = make([][]byte, len(readers))
		ids     = make(map[uuid.UUID]int, len(readers))
	)

	// Advance moves the next record from a reader into our state.
	advance := func(i int) error {
		if ok := scanner[i].Scan(); ok {
			if records[i] = scanner[i].Bytes(); len(records[i]) < 0 {
				return nil
			}

			fields := bytes.Fields(records[i])
			if len(fields) < 1 {
				return errInvalidUUID{errors.Errorf("missing uuid")}
			}

			id, err := uuid.ParseBytes(fields[0])
			if err != nil {
				return errInvalidUUID{errors.Errorf("invalid uuid")}
			}
			ids[id] = i

		} else if err := scanner[i].Err(); err != nil && err != io.EOF {
			return err
		}
		return nil
	}

	// Initialize all of the scanners and their first record.
	for i := 0; i < len(readers); i++ {
		scanner[i] = bufio.NewScanner(readers[i])
		scanner[i].Split(scanLinesPreserveNewline)
		if err := advance(i); err != nil {
			return n, err
		}
	}

	sw := sizedWriter{w, 0}
	for k, v := range ids {
		// if id already exists, skip it
		if i, ok := ids[k]; ok && v != i {
			continue
		}

		if _, err := sw.Write(records[v]); err != nil {
			return n, err
		}
	}

	return sw.n, nil
}

type sizedWriter struct {
	w io.Writer
	n int64
}

func (sw *sizedWriter) Write(p []byte) (int, error) {
	n, err := sw.w.Write(p)
	if err != nil {
		return 0, err
	}

	sw.n += int64(n)
	return len(p), nil
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
