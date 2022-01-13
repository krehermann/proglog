package log

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "github.com/krehermann/proglog/api/v1"
)

//Log manages the list of segments
type Log struct {
	mu            sync.RWMutex
	Dir           string
	Cfg           Config
	activeSegment *segment
	segments      []*segment
}

var defaultSize = uint64(1024)

func NewLog(dir string, cfg Config) (*Log, error) {
	if cfg.Segment.MaxIndexBytes == 0 {
		cfg.Segment.MaxIndexBytes = defaultSize
	}
	if cfg.Segment.MaxStoreBytes == 0 {
		cfg.Segment.MaxStoreBytes = defaultSize
	}

	l := &Log{
		Dir:      dir,
		Cfg:      cfg,
		segments: make([]*segment, 0),
	}

	err := l.initialize()
	if err != nil {
		return nil, err
	}
	return l, nil
}

//initialize finds all the segments in the configured directory and sets activeSegment
//to that specified in the configuration. If no segments exist, one is created in Dir
//using the configured InitialOffset
func (l *Log) initialize() error {
	storeFiles := make([]string, 0)
	indexFiles := make([]string, 0)
	filepath.WalkDir(l.Dir, func(path string, d fs.DirEntry, err error) error {
		if d.IsDir() {
			return nil
		}

		if filepath.Ext(path) == storeExt {
			storeFiles = append(storeFiles, filepath.Base(path))
		}
		if filepath.Ext(path) == indexExt {
			indexFiles = append(indexFiles, filepath.Base(path))
		}
		return nil
	})
	sort.Strings(storeFiles)
	sort.Strings(indexFiles)
	if len(storeFiles) != len(indexFiles) {
		return fmt.Errorf("mismatch store and index files %v, %v", storeFiles, indexFiles)
	}

	baseOffsets := make([]uint64, len(storeFiles))
	var err error
	for i, _ := range storeFiles {
		storePrefix := strings.TrimSuffix(storeFiles[i], storeExt)
		indexPrefix := strings.TrimSuffix(indexFiles[i], indexExt)
		if storePrefix != indexPrefix {
			return fmt.Errorf("store and index name mismatch at %d: %v %v", i, storePrefix, indexPrefix)
		}
		baseOffsets[i], err = strconv.ParseUint(storePrefix, 10, 0)

		if err != nil {
			return err
		}
	}
	//sort offsets as numbers
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})
	for _, off := range baseOffsets {
		err := l.newSegment(off)
		if err != nil {
			return err
		}
	}
	if len(l.segments) == 0 {
		err = l.newSegment(l.Cfg.Segment.InitialOffset)
		if err != nil {
			return err
		}
	}
	return nil

}

//newSegment is wrapper that creates a segment and manages updating the active segment and segment list
func (l *Log) newSegment(off uint64) error {
	s, err := newSegment(l.Dir, off, l.Cfg)
	if err != nil {
		return err
	}
	l.activeSegment = s
	l.segments = append(l.segments, s)
	return nil
}

//Append appends a record to the log and returns the offset in the current segment
//After appending, if the active segment is full a new segment is created for future appends
func (l *Log) Append(r *api.Record) (uint64, error) {
	off, err := l.activeSegment.Append(r)
	if err != nil {
		return 0, err
	}
	if l.activeSegment.IsFull() {
		err = l.newSegment(off + 1)
		if err != nil {
			return 0, err
		}
	}
	return off, err
}

//Reads reads the record stored at the given offset.
//Finds the appropriate segment from which to read and return an error if out of bounds
func (l *Log) Read(off uint64) (*api.Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	//find the appropriate segment
	var s *segment
	for _, seg := range l.segments {
		if seg.baseOffset <= off && off < seg.nextOffset {
			s = seg
			break
		}
	}

	if s == nil || s.nextOffset <= off {
		return nil, api.ErrOffsetOutOfRange{Offset: off}
	}
	return s.Read(off)
}

//Close closes all the segments backing the log
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, s := range l.segments {
		err := s.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (l *Log) Size() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := int64(0)
	for _, segment := range l.segments {
		s += segment.str.Size()
	}
	return s
}

//Remove deletes the configured directory
func (l *Log) Remove() error {
	err := l.Close()
	if err != nil {
		return err
	}
	return os.RemoveAll(l.Dir)
}

//Reset removes all segments and recreates a new one
func (l *Log) Reset() error {
	err := l.Remove()
	if err != nil {
		return err
	}
	return l.initialize()
}

//LowestOffset returns the smallest offset in the log
func (l *Log) LowestOffset() (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.segments[0].baseOffset, nil
}

//HighestOffset returns the largest offset in the log
func (l *Log) HighestOffset() (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	next := l.segments[len(l.segments)-1].nextOffset
	if next == 0 {
		return next, nil
	}
	return next - 1, nil
}

//Truncate removes all segments whose highest offset is lower than lowest
func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	//segments are sorted, so count how many are removed and break
	deleteIdx := 0
	for i, s := range l.segments {
		if s.nextOffset-1 <= lowest {
			err := s.Remove()
			if err != nil {
				return err
			}
			deleteIdx = i
		} else {
			break
		}
	}
	l.segments = l.segments[deleteIdx+1:]
	return nil
}

//Reader returns an io.Reader for the whole Log
func (l *Log) Reader() io.Reader {
	l.mu.Lock()
	defer l.mu.Unlock()
	readers := make([]io.Reader, len(l.segments))
	for i, s := range l.segments {
		readers[i] = newOriginReader(s.str, 0)
	}
	return io.MultiReader(readers...)
}

//originReader is a wrapper around a store that
// statisfies the io.Reader interface
// and enables reading the store from beginning to end
type originReader struct {
	*store
	offset int64
}

func newOriginReader(s *store, offset int64) *originReader {
	return &originReader{
		store:  s,
		offset: offset,
	}
}

//Read reads from the store at current offset and updates offset
func (or *originReader) Read(p []byte) (int, error) {
	n, err := or.ReadAt(p, or.offset)
	or.offset += int64(n)
	return n, err
}
