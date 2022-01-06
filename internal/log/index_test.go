package log

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	f, err := ioutil.TempFile("", "index_test")
	require.NoError(t, err)
	defer os.Remove((f.Name()))

	config := Config{}
	config.Segment.MaxIndexBytes = 1024
	idx, err := newIndex(f, config)
	require.NoError(t, err)
	require.Equal(t, idx.Name(), f.Name())

	_, _, err = idx.Read(-1)
	require.ErrorIs(t, err, io.EOF)

	entries := []struct {
		Off uint32
		Pos uint64
	}{
		{Off: 0, Pos: 0},
		{Off: 1, Pos: 10},
	}

	for _, want := range entries {
		require.NoError(t, idx.Write(want.Off, want.Pos))
		off, pos, err := idx.Read(int64(want.Off))
		require.NoError(t, err)
		require.Equal(t, want.Off, off)
		require.Equal(t, want.Pos, pos)
	}

	//EOF after last record
	_, _, err = idx.Read(int64(len(entries)))
	require.ErrorIs(t, err, io.EOF)

	//rebuild idx from file
	require.NoError(t, idx.Close())

	f, err = os.OpenFile(idx.Name(), os.O_RDWR, 0600)
	require.NoError(t, err)
	idx2, err := newIndex(f, config)
	require.NoError(t, err)
	off, pos, err := idx2.Read(-1)
	require.NoError(t, err)
	require.Equal(t, off, entries[len(entries)-1].Off)
	require.Equal(t, pos, entries[len(entries)-1].Pos)

}
