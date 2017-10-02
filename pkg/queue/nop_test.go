package queue

import (
	"testing"
	"testing/quick"
)

func TestNopQueue(t *testing.T) {
	t.Parallel()

	t.Run("enqueue", func(t *testing.T) {
		fn := func(b []byte) bool {
			queue := newNopQueue()
			w, err := queue.Enqueue()
			if err != nil {
				t.Fatal(err)
			}

			n, err := w.Write(b)
			if err != nil {
				t.Fatal(err)
			}

			return n == 0
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("sync returns nil", func(t *testing.T) {
		fn := func(b []byte) bool {
			queue := newNopQueue()
			w, err := queue.Enqueue()
			if err != nil {
				t.Fatal(err)
			}

			if _, err := w.Write(b); err != nil {
				t.Fatal(err)
			}

			return w.Sync() == nil
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("close returns nil", func(t *testing.T) {
		fn := func(b []byte) bool {
			queue := newNopQueue()
			w, err := queue.Enqueue()
			if err != nil {
				t.Fatal(err)
			}

			if _, err := w.Write(b); err != nil {
				t.Fatal(err)
			}

			return w.Close() == nil
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("delete resets the write segment", func(t *testing.T) {
		fn := func(b []byte) bool {
			queue := newNopQueue()
			w, err := queue.Enqueue()
			if err != nil {
				t.Fatal(err)
			}

			if _, err := w.Write(b); err != nil {
				t.Fatal(err)
			}

			if err := w.Delete(); err != nil {
				t.Fatal(err)
			}

			return w.Size() == 0
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("dequeue", func(t *testing.T) {
		fn := func(b []byte) bool {
			queue := newNopQueue()
			w, err := queue.Enqueue()
			if err != nil {
				t.Fatal(err)
			}

			n, err := w.Write(b)
			if err != nil {
				t.Fatal(err)
			}

			r, err := queue.Dequeue()
			if err != nil {
				t.Fatal(err)
			}

			res := make([]byte, len(b))
			n, err = r.Read(res)
			if err != nil {
				t.Error(err)
			}

			return n == 0
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("dequeue empty queue", func(t *testing.T) {
		queue := newNopQueue()
		_, err := queue.Dequeue()
		if expected, actual := true, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("dequeue then commit returns nil", func(t *testing.T) {
		fn := func(b []byte) bool {
			queue := newNopQueue()
			w, err := queue.Enqueue()
			if err != nil {
				t.Fatal(err)
			}

			if _, err := w.Write(b); err != nil {
				t.Fatal(err)
			}

			r, err := queue.Dequeue()
			if err != nil {
				t.Fatal(err)
			}

			return r.Commit() == nil
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("dequeue then failed returns nil", func(t *testing.T) {
		fn := func(b []byte) bool {
			queue := newNopQueue()
			w, err := queue.Enqueue()
			if err != nil {
				t.Fatal(err)
			}

			if _, err := w.Write(b); err != nil {
				t.Fatal(err)
			}

			r, err := queue.Dequeue()
			if err != nil {
				t.Fatal(err)
			}

			return r.Failed() == nil
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}
