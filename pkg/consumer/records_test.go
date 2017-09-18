package consumer

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"strings"
	"testing"
	"testing/quick"

	"github.com/trussle/snowy/pkg/uuid"
)

func TestMergeRecords(t *testing.T) {
	t.Parallel()

	t.Run("no readers", func(t *testing.T) {
		w := writer{func([]byte) (int, error) {
			t.Fatal("failed if called")
			return 0, nil
		}}
		n, err := mergeRecords(w)
		if err != nil {
			t.Fatal(err)
		}

		if expected, actual := int64(0), n; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})

	t.Run("nil readers", func(t *testing.T) {
		w := writer{func([]byte) (int, error) {
			t.Fatalf("failed if called")
			return 0, nil
		}}
		n, err := mergeRecords(w, nil)
		if err != nil {
			t.Fatal(err)
		}

		if expected, actual := int64(0), n; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})

	t.Run("reader", func(t *testing.T) {
		fn := func(id uuid.UUID, b []byte) bool {
			if len(b) == 0 {
				return true
			}

			var (
				enc = base64.StdEncoding.EncodeToString(b)
				r   = strings.NewReader(fmt.Sprintf("%s %s\n", id.String(), enc))
			)

			w := writer{func(p []byte) (int, error) {
				if expected, actual := id.String(), string(bytes.Fields(p)[0]); expected != actual {
					t.Fatalf("expected: %s, actual: %s", expected, actual)
				}
				if expected, actual := enc, string(bytes.Fields(p)[1]); expected != actual {
					t.Fatalf("expected: %s, actual: %s", expected, actual)
				}
				return len(p), nil
			}}

			n, err := mergeRecords(w, r)
			if err != nil {
				t.Fatal(err)
			}

			if expected, actual := int64(r.Size()), n; expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	ids := make([]string, 10)
	for k := range ids {
		ids[k] = uuid.New().String()
	}

	testcases := []struct {
		name   string
		input  [][]string
		output []string
	}{
		{
			name:   "nil input reader",
			input:  nil,
			output: nil,
		},
		{
			name:   "empty input reader",
			input:  [][]string{},
			output: []string{},
		},
		{
			name: "single input reader",
			input: [][]string{
				{fmt.Sprintf("%s Foo", ids[0])},
			},
			output: []string{
				fmt.Sprintf("%s Foo", ids[0]),
			},
		},
		{
			name: "multiple input reader",
			input: [][]string{
				{fmt.Sprintf("%s Foo", ids[0])},
				{fmt.Sprintf("%s Bar", ids[1])},
			},
			output: []string{
				fmt.Sprintf("%s Foo", ids[0]),
				fmt.Sprintf("%s Bar", ids[1]),
			},
		},
		{
			name: "multiple inputs reader",
			input: [][]string{
				{fmt.Sprintf("%s Foo", ids[0]), fmt.Sprintf("%s Bar", ids[1])},
				{fmt.Sprintf("%s Baz", ids[2])},
			},
			output: []string{
				fmt.Sprintf("%s Foo", ids[0]),
				fmt.Sprintf("%s Bar", ids[1]),
				fmt.Sprintf("%s Baz", ids[2]),
			},
		},
	}

	for k, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			readers := make([]io.Reader, len(testcase.input))
			for i, slice := range testcase.input {
				str := strings.Join(slice, "\n")
				if str != "" {
					str += "\n"
				}
				readers[i] = strings.NewReader(str)
			}

			var buf bytes.Buffer
			if _, err := mergeRecords(&buf, readers...); err != nil {
				t.Error(err)
				return
			}

			outputStr := strings.Join(testcase.output, "\n")
			if outputStr != "" {
				outputStr += "\n"
			}
			if expected, actual := outputStr, buf.String(); expected != actual {
				t.Errorf("%d: expected %s, actual %s", k, expected, actual)
			}
		})
	}
}

type writer struct {
	fn func([]byte) (int, error)
}

func (w writer) Write(p []byte) (int, error) {
	return w.fn(p)
}
