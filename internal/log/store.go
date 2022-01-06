package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

// lenWidth is the length prefix of a record. 8 is the byte size of uint64
const lenWidth = 8

type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, err
}

//Append adds p bytes to the store
// Returns
//  n: number of bytes written. This is the sum of length of p + length prefix
//  pos: starting position of p in the store
//  err: any error encountered
//If an error is encountered, then n, pos are set returned as 0
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos = s.size
	err = binary.Write(s.buf, enc, uint64(len(p)))
	if err != nil {
		return 0, 0, err
	}
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}
	w += lenWidth
	s.size += uint64(w)
	return uint64(w), pos, nil
}

//Read retrieves a record at pos
// Returns
//  []byte slice of bytes containing the record
//  error any error that is encountered
// If an error is encountered, the byte slice is nil
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	//Flush to ensure all records are written to backing file before reading at pos
	err := s.buf.Flush()
	if err != nil {
		return nil, err
	}
	size := make([]byte, lenWidth)
	_, err = s.File.ReadAt(size, int64(pos))
	if err != nil {
		return nil, err
	}
	buf := make([]byte, enc.Uint64(size))
	_, err = s.File.ReadAt(buf, int64(pos+lenWidth))
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.buf.Flush()
	if err != nil {
		return 0, err
	}
	return s.File.ReadAt(p, off)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.buf.Flush()
	if err != nil {
		return err
	}
	return s.File.Close()
}

func (s *store) Size() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int64(s.size)
}
