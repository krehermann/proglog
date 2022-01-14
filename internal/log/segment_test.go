package log

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"testing"

	api "github.com/krehermann/proglog/api/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSegment(t *testing.T) {
	d, err := ioutil.TempDir("", "test-segment")
	require.NoError(t, err)
	cfg := Config{}
	idxCap := 3
	cfg.Segment.MaxIndexBytes = entWidth * uint64(idxCap)
	cfg.Segment.MaxStoreBytes = 1024
	base := uint64(4)
	s, err := newSegment(d, base, cfg)

	require.Equal(t, s.baseOffset, base)
	require.Equal(t, s.nextOffset, base)
	require.False(t, s.IsFull())

	for i := uint64(0); i < uint64(idxCap); i++ {
		want := &api.Record{
			Value:  []byte(fmt.Sprintf("record_%d", i)),
			Offset: i,
		}
		off, err := s.Append(want)
		assert.NoError(t, err)
		assert.Equal(t, off, base+i)

		got, err := s.Read(off)
		assert.NoError(t, err, " loop %d", i)
		assert.Equal(t, 0, bytes.Compare(want.Value, got.Value))
	}
	//idx should be full
	assert.True(t, s.IsFull())
	_, err = s.Append(&api.Record{})
	assert.ErrorIs(t, err, io.EOF)

	//test for full store
	assert.NoError(t, s.Close())
	cfg.Segment.MaxStoreBytes = s.str.size
	cfg.Segment.MaxIndexBytes = 1024
	s, err = newSegment(d, base, cfg)
	assert.NoError(t, err)
	assert.True(t, s.IsFull())

	//delete and recreate empty segment
	err = s.Remove()
	assert.NoError(t, err)
	s, err = newSegment(d, base, cfg)
	assert.NoError(t, err)
	assert.False(t, s.IsFull())
}
