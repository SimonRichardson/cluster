package queue

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/SimonRichardson/cluster/pkg/fs"
	"github.com/SimonRichardson/cluster/pkg/uuid"
	"github.com/pkg/errors"
)

//Extension describe differing types of persisted queued types
type Extension string

const (

	// Active states which items are currently actively being worked on
	Active Extension = ".active"

	// Flushed states which items have been flushed
	Flushed Extension = ".flushed"

	// Pending status which items are pending
	Pending Extension = ".pending"
)

// Ext returns the extension of the constant extension
func (e Extension) Ext() string {
	return string(e)
}

const (
	lockFile = "LOCK"
)

type realQueue struct {
	root     string
	filesys  fs.Filesystem
	releaser fs.Releaser
}

func newRealQueue(filesys fs.Filesystem, root string) (Queue, error) {
	if err := filesys.MkdirAll(root); err != nil {
		return nil, errors.Wrapf(err, "creating path %s", root)
	}

	lock := filepath.Join(root, lockFile)
	r, _, err := filesys.Lock(lock)
	if err != nil {
		return nil, errors.Wrapf(err, "locking %s", lock)
	}
	if err := recoverSegments(filesys, root); err != nil {
		return nil, errors.Wrap(err, "during recovery")
	}

	return &realQueue{
		root:     root,
		filesys:  filesys,
		releaser: r,
	}, nil
}

func (q *realQueue) Enqueue() (WriteSegment, error) {
	id, err := uuid.New()
	if err != nil {
		return nil, errors.Wrap(err, "enqueue")
	}
	filename := filepath.Join(q.root, fmt.Sprintf("%s%s", id, Active))

	f, err := q.filesys.Create(filename)
	if err != nil {
		return nil, err
	}

	return realWriteSegment{q.filesys, f}, nil
}

func (q *realQueue) Dequeue() (ReadSegment, error) {
	var (
		oldest = time.Now()
		chosen string
	)
	q.filesys.Walk(q.root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || filepath.Ext(path) != Flushed.Ext() {
			return nil
		}
		if t := info.ModTime(); t.Before(oldest) {
			chosen, oldest = path, t
		}
		return nil
	})
	if chosen == "" {
		return nil, errNoSegmentsAvailable{errors.New("nothing found for reading")}
	}

	newname := modifyExtension(chosen, Pending.Ext())
	if err := q.filesys.Rename(chosen, newname); err != nil {
		return nil, errors.Wrap(err, "error when fetching; please try again")
	}

	f, err := q.filesys.Open(newname)
	if err != nil {
		if renameErr := q.filesys.Rename(newname, chosen); renameErr != nil {
			return nil, errors.Wrap(renameErr, "error attempting to rename")
		}
		return nil, err
	}

	return realReadSegment{q.filesys, f}, nil
}

func (q *realQueue) Close() error {
	return q.releaser.Release()
}

type realWriteSegment struct {
	fs fs.Filesystem
	f  fs.File
}

func (w realWriteSegment) Write(p []byte) (int, error) {
	return w.f.Write(p)
}

func (w realWriteSegment) Sync() error {
	return w.f.Sync()
}

func (w realWriteSegment) Close() error {
	if err := w.f.Close(); err != nil {
		return err
	}

	var (
		oldname = w.f.Name()
		newname = modifyExtension(oldname, Flushed.Ext())
	)
	return w.fs.Rename(oldname, newname)
}

func (w realWriteSegment) Delete() error {
	if err := w.f.Close(); err != nil {
		return err
	}
	return w.fs.Remove(w.f.Name())
}

func (w realWriteSegment) Size() int64 {
	return w.f.Size()
}

type realReadSegment struct {
	fs fs.Filesystem
	f  fs.File
}

func (r realReadSegment) Read(p []byte) (int, error) {
	return r.f.Read(p)
}

func (r realReadSegment) Commit() error {
	if err := r.f.Close(); err != nil {
		return err
	}
	return r.fs.Remove(r.f.Name())
}

func (r realReadSegment) Failed() error {
	if err := r.f.Close(); err != nil {
		return err
	}

	var (
		oldname = r.f.Name()
		newname = modifyExtension(oldname, Flushed.Ext())
	)
	return r.fs.Rename(oldname, newname)
}

func (r realReadSegment) Size() int64 {
	return r.f.Size()
}

func recoverSegments(filesys fs.Filesystem, root string) error {
	var toRename []string
	filesys.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		switch filepath.Ext(path) {
		case Active.Ext(), Pending.Ext():
			toRename = append(toRename, path)
		}
		return nil
	})

	for _, path := range toRename {
		var (
			oldname = path
			newname = modifyExtension(oldname, Flushed.Ext())
		)
		if err := filesys.Rename(oldname, newname); err != nil {
			return err
		}
	}
	return nil
}

func modifyExtension(filename, newExt string) string {
	return filename[:len(filename)-len(filepath.Ext(filename))] + newExt
}
