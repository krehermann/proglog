package log

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	api "github.com/krehermann/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

//segment wraps index and store to cooridinate operations across the two.
//segment coordinates writes -- appending to log and inserting to index
// and reads -- lookup in index and read from store
type segment struct {
	str                    *store
	idx                    *index
	cfg                    Config
	baseOffset, nextOffset uint64
}

var (
	storeExt = ".store"
	indexExt = ".index"
)

func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		cfg:        c,
		baseOffset: baseOffset,
		nextOffset: baseOffset,
	}
	storeFile := filepath.Join(dir, fmt.Sprintf("%d%s", baseOffset, storeExt))
	sf, err := os.OpenFile(storeFile, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	s.str, err = newStore(sf)
	if err != nil {
		return nil, err
	}
	idxF, err := os.OpenFile(
		filepath.Join(dir, fmt.Sprintf("%d%s", baseOffset, indexExt)),
		os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	s.idx, err = newIndex(idxF, s.cfg)
	if err != nil {
		return nil, err
	}
	off, _, err := s.idx.Read(-1)
	// EOF is not an error condition, just means no data
	if err != nil {
		if !errors.Is(err, io.EOF) {
			return nil, err
		}
	} else {
		// if we didn't get EOF, then there is an index entry. Set next to one after it.
		s.nextOffset = s.baseOffset + uint64(off) + 1
	}

	return s, nil
}

//Append write record to segment and returns appened record's offset
func (s *segment) Append(r *api.Record) (offset uint64, err error) {
	cur := s.nextOffset
	r.Offset = cur
	p, err := proto.Marshal(r)
	if err != nil {
		return 0, err
	}
	_, pos, err := s.str.Append(p)
	if err != nil {
		return 0, err
	}
	err = s.idx.Write(
		//offset in idx are relative to base offset
		uint32(s.nextOffset-s.baseOffset),
		pos)
	if err != nil {
		return 0, err
	}
	s.nextOffset++
	return r.Offset, nil
}

//Read returns the record given the offset
//offset is the absolute offset
func (s *segment) Read(off uint64) (*api.Record, error) {
	// need to translate absolute offset to relative in this segment
	_, pos, err := s.idx.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}
	buf, err := s.str.Read(pos)
	if err != nil {
		return nil, err
	}
	r := &api.Record{}
	err = proto.Unmarshal(buf, r)
	return r, err
}

//IsFull returns true if either the index or store are equal/greater than their respective configured values
func (s *segment) IsFull() bool {
	return s.str.size >= s.cfg.Segment.MaxStoreBytes || s.idx.size >= s.cfg.Segment.MaxIndexBytes
}

func (s *segment) Remove() error {
	err := s.Close()
	if err != nil {
		return err
	}
	err = os.Remove(s.idx.Name())
	if err != nil {
		return err
	}
	return os.Remove(s.str.Name())

}

func (s *segment) Close() error {
	err := s.idx.Close()
	if err != nil {
		return err
	}
	return s.str.Close()
}

//nearestMultiple returns the nearest and lesser multiple of k in j
func nearestMultiple(j, k uint64) uint64 {
	return (j / k) * k
}
