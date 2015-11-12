package watch

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Op uint32

const defaultPollFrequency = time.Millisecond * 250

const (
	Create Op = 1 << iota
	Write
	Remove
	Rename
	Chmod
)

type Event struct {
	Name string
	Op   Op
}

type watch struct {
	path  string
	files map[string]FileInfo
}

type Watcher struct {
	Events        chan Event
	Errors        chan error
	pollFrequency time.Duration
	ticker        *time.Ticker
	watches       map[string]watch
	watchesMutex  sync.Mutex
	isRunning     bool
}

type FileInfo struct {
	os.FileInfo
	path string
}

func directoryMap(path string) (map[string]FileInfo, error) {

	_, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	m := make(map[string]FileInfo)

	filepath.Walk(path, func(path string, f os.FileInfo, err error) (e error) {
		m[f.Name()] = FileInfo{f, path}
		return nil
	})

	return m, nil
}

func NewWatcher() (*Watcher, error) {
	w := &Watcher{
		pollFrequency: defaultPollFrequency,
		Events:        make(chan Event),
		Errors:        make(chan error),
		watches:       make(map[string]watch),
		isRunning:     false,
	}

	w.start()
	return w, nil
}

// Set a new poll frequency. Doesn't effect currently running watches, and defaults to 250ms.
func (w *Watcher) PollFrequency(f time.Duration) error {
	w.pollFrequency = f
	return nil
}

// Add a watch for the given file or directory (non-recursive).
func (w *Watcher) Add(path string) error {
	if path == "" {
		return errors.New("path cannot be empty")
	}
	if _, ok := w.watches[path]; ok {
		return errors.New("watch already exists")
	}

	files, err := directoryMap(path)
	if err != nil {
		return err
	}

	w.watchesMutex.Lock()
	w.watches[path] = watch{path: path, files: files}
	w.watchesMutex.Unlock()

	return nil
}

// Remove the watch identified by the given path.
func (w *Watcher) Remove(path string) error {
	if path == "" {
		return errors.New("can't remove an empty path")
	}

	if _, ok := w.watches[path]; ok {
		delete(w.watches, path)
	}

	return nil

}

// Stop all watches and close all channels.
func (w *Watcher) Close() {
	if w.isRunning {
		w.ticker.Stop()
		w.isRunning = false
		close(w.Events)
		close(w.Errors)
	}
}

func (e Event) String() string {
	events := ""

	if e.Op&Create == Create {
		events += "|CREATE"
	}
	if e.Op&Remove == Remove {
		events += "|REMOVE"
	}
	if e.Op&Write == Write {
		events += "|WRITE"
	}
	if e.Op&Rename == Rename {
		events += "|RENAME"
	}
	if e.Op&Chmod == Chmod {
		events += "|CHMOD"
	}

	if len(events) > 0 {
		events = events[1:]
	}

	return fmt.Sprintf("%q: %s", e.Name, events)
}
func (w *Watcher) start() {
	w.ticker = time.NewTicker(w.pollFrequency)
	w.isRunning = true
	go func() {
		for _ = range w.ticker.C {
			for _, curWatch := range w.watches {
				n, err := directoryMap(curWatch.path)
				if err != nil {
					w.Errors <- err
				}

				// look for new and modified files
				for k, v := range n {
					f, ok := curWatch.files[k]

					if !ok {
						// new event
						w.Events <- Event{Name: v.path, Op: Create}
					} else {
						// modified event
						if f.ModTime() != v.ModTime() {
							w.Events <- Event{Name: v.path, Op: Write}
						}
						// chmod event
						if f.Mode() != v.Mode() {
							w.Events <- Event{Name: v.path, Op: Chmod}
						}
					}
				}

				// and deleted files
				for k, v := range curWatch.files {
					_, ok := n[k]
					if !ok {
						// remove event
						w.Events <- Event{Name: v.path, Op: Remove}
					}
				}

				w.watchesMutex.Lock()
				w.watches[curWatch.path] = watch{path: curWatch.path, files: n}
				w.watchesMutex.Unlock()
			}
		}
	}()
}
