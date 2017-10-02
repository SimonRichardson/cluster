package consumer

import (
	"bufio"
	"bytes"
	"io"

	"github.com/SimonRichardson/cluster/pkg/uuid"
	"github.com/pkg/errors"
)

// mergeRecords will merge multiple readers into one
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
	reader := io.MultiReader(notnil...)

	// Initialize our state.
	var (
		records [][]byte
		ids     = map[uuid.UUID]int{}
	)

	// Initialize the scanner
	scanner := bufio.NewScanner(reader)
	scanner.Split(scanLinesPreserveNewline)

	for {
		if ok := scanner.Scan(); ok {
			record := scanner.Bytes()
			if len(record) == 0 {
				continue
			}

			fields := bytes.Fields(record)
			if len(fields) == 0 {
				return 0, errInvalidUUID{errors.Errorf("missing uuid")}
			}

			id, err := uuid.ParseBytes(fields[0])
			if err != nil {
				return 0, errInvalidUUID{errors.Errorf("invalid uuid")}
			}

			if _, ok := ids[id]; !ok {
				records = append(records, record)
				ids[id] = len(records) - 1
			}
			continue

		} else if err := scanner.Err(); err != nil && err != io.EOF {
			return 0, err
		}

		break
	}

	sw := sizedWriter{w, 0}
	for _, v := range records {
		if _, err := sw.Write(v); err != nil {
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
