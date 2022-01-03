package server

import (
	"fmt"
	"sync"
)

var ErrOffsetNotFound = fmt.Errorf("offset not found")

//Log is a commit log implementation.
type Log struct {
	mu      sync.Mutex
	records []Record
}

func NewLog() *Log {
	return &Log{}
}

//Append appends a record to the Log and returns the offset and any error
func (l *Log) Append(r Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	offset := uint64(len(l.records))
	r.Offset = offset
	l.records = append(l.records, r)
	return offset, nil
}

//Read fetches record at given offset. Return ErrOffsetNotFound if out of bounds.
func (l *Log) Read(offset uint64) (Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if offset >= uint64(len(l.records)) {
		return Record{}, ErrOffsetNotFound
	}
	return l.records[offset], nil
}

type Record struct {
	Value  []byte `json:"value"`
	Offset uint64 `json:"offset"`
}
