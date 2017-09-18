package consumer

import "io"

func mergeRecords(w io.Writer, readers ...io.Reader) (n int64, err error) {
	if len(readers) == 0 {
		return 0, nil
	}

	return n, nil
}
