package log

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"

	api "github.com/krehermann/proglog/api/v1"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T, log *Log,
	){
		"append and read":             testAppendRead,
		"offest out of range error":   testOutOfRangeErr,
		"init with existing segments": testInitExisting,
		"reader":                      testReader,
		"truncate":                    testTruncate,
	} {
		t.Run(scenario, func(t *testing.T) {
			dir, err := ioutil.TempDir("", "log-test")
			assert.NoError(t, err)
			defer os.RemoveAll(dir)
			cfg := Config{}
			cfg.Segment.MaxStoreBytes = 1024
			l, err := NewLog(dir, cfg)
			assert.NoError(t, err)
			fn(t, l)
		})
	}
}

func testAppendRead(t *testing.T, log *Log) {
	r := &api.Record{
		Value: []byte("a long habit of not thinkinga thing wrong, gives it a superficial appearance of being right"),
	}

	off, err := log.Append(r)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), off)
	got, err := log.Read(off)
	assert.NoError(t, err)
	assert.Equal(t, 0, bytes.Compare(r.Value, got.Value))

}

func testOutOfRangeErr(t *testing.T, log *Log) {
	r, err := log.Read(1)
	assert.ErrorIs(t, err, api.ErrOffsetOutOfRange{})
	assert.Equal(t, 1, err.(api.ErrOffsetOutOfRange).Offset)
	assert.Nil(t, r)
}

func testInitExisting(t *testing.T, log *Log) {
	r := &api.Record{
		Value: []byte("When men yield up the privilege of thinking, the last shadow of liberty quits the horizon."),
	}
	for i := 0; i < 3; i++ {
		_, err := log.Append(r)
		assert.NoError(t, err)
	}

	assert.NoError(t, log.Close())
	off, err := log.LowestOffset()
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), off)
	off, err = log.HighestOffset()
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), off)

	logReload, err := NewLog(log.Dir, log.Cfg)
	assert.NoError(t, err)
	off, err = logReload.LowestOffset()
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), off)
	off, err = logReload.HighestOffset()
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), off)
}

func testReader(t *testing.T, log *Log) {
	want := &api.Record{
		Value: []byte("Thomas Paine, Common Sense"),
	}
	//reconfigure the log so each segment's index contains at most one entry
	cfg := Config{}
	cfg.Segment.MaxIndexBytes = entWidth
	assert.NoError(t, log.Close())
	log, err := NewLog(log.Dir, cfg)
	assert.NoError(t, err)
	nSegments := 3
	for i := 0; i < nSegments; i++ {
		off, err := log.Append(want)
		assert.NoError(t, err)
		r, err := log.Read(off)
		assert.NoError(t, err)
		assert.Equal(t, r.Value, want.Value, "append loop %d", i)
	}
	assert.Equal(t, nSegments+1, len(log.segments))

	reader := log.Reader()
	b, err := ioutil.ReadAll(reader)
	assert.NoError(t, err)
	assert.Equal(t, log.Size(), int64(len(b)))
	prevEnd := 0
	for i := 0; i < nSegments; i++ {
		record := &api.Record{}
		rStart := uint64(prevEnd)
		rLen := enc.Uint64(b[rStart : rStart+lenWidth])
		end := rStart + lenWidth + rLen
		assert.NoError(t,
			proto.Unmarshal(b[rStart+lenWidth:end], record),
			"loop %d", i)
		assert.Equal(t, want.Value, record.Value, "loop %d", i)
		prevEnd = int(end)
	}
}

func testTruncate(t *testing.T, log *Log) {
	want := &api.Record{
		Value: []byte("Society in every state is a blessing, but government, even in its best state, is but a necessary evil"),
	}
	//reconfigure the log so each segment's index contains at most one entry
	cfg := Config{}
	cfg.Segment.MaxIndexBytes = entWidth
	assert.NoError(t, log.Close())
	log, err := NewLog(log.Dir, cfg)
	assert.NoError(t, err)

	for i := 0; i < 4; i++ {
		_, err := log.Append(want)
		assert.NoError(t, err)
	}
	assert.NoError(t, log.Truncate(1))
	_, err = log.Read(0)
	assert.Error(t, err)

	got, err := log.Read(2)
	assert.NoError(t, err)
	assert.Equal(t, got.Value, want.Value)
}
