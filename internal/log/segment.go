package log

import (
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

func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		cfg:        c,
		baseOffset: baseOffset,
		nextOffset: baseOffset,
	}
	storeFile := filepath.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store"))
	sf, err := os.OpenFile(storeFile, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	s.str, err = newStore(sf)
	if err != nil {
		return nil, err
	}
	idxF, err := os.OpenFile(
		filepath.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
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
	if err != nil && err != io.EOF {
		return nil, err
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

func (s *segment) Read(off uint64) (r *api.Record, err error) {
	_, pos, err := s.idx.Read(int64(off))
	if err != nil {
		return nil, err
	}
	buf, err := s.str.Read(pos)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(buf, r)
	return r, err
}
