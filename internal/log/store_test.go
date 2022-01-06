package log

import (
	"bytes"
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func newtestStore(t *testing.T) *store {
	f, err := ioutil.TempFile("", "")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)
	return s
}

func Test_store_Append(t *testing.T) {

	commonStore := newtestStore(t)
	type fields struct {
		s *store
	}
	type args struct {
		p []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantN   uint64
		wantPos uint64
		wantErr bool
	}{
		// TODO: Add test cases.
		{
			name: "append hi",
			fields: fields{
				s: commonStore,
			},
			args: args{
				p: []byte("hi"),
			},
			wantN:   uint64(len([]byte("hi")) + lenWidth),
			wantPos: 0,
			wantErr: false,
		},
		{
			name: "append there",
			fields: fields{
				s: commonStore,
			},
			args: args{
				p: []byte("there"),
			},
			wantN:   uint64(len([]byte("there")) + lenWidth),
			wantPos: uint64(len([]byte("hi")) + lenWidth), // dependent on previous append
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			gotN, gotPos, err := tt.fields.s.Append(tt.args.p)
			if (err != nil) != tt.wantErr {
				t.Errorf("store.Append() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotN != tt.wantN {
				t.Errorf("store.Append() gotN = %v, want %v", gotN, tt.wantN)
			}
			if gotPos != tt.wantPos {
				t.Errorf("store.Append() gotPos = %v, want %v", gotPos, tt.wantPos)
			}
		})
	}
}

func Test_store_Read(t *testing.T) {

	commonStore := newtestStore(t)

	record1 := []byte("hola amigo")
	_, pos1, err := commonStore.Append(record1)
	require.NoError(t, err)

	record2 := []byte("hola amiga")
	_, pos2, err := commonStore.Append(record2)
	require.NoError(t, err)

	type fields struct {
		s *store
	}

	type args struct {
		pos uint64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []byte
		wantErr bool
	}{
		// TODO: Add test cases.
		{
			name: "read first",
			fields: fields{
				s: commonStore,
			},
			args: args{
				pos: pos1,
			},
			want:    record1,
			wantErr: false,
		},
		{
			name: "read second",
			fields: fields{
				s: commonStore,
			},
			args: args{
				pos: pos2,
			},
			want:    record2,
			wantErr: false,
		},
		{
			name: "bad position",
			fields: fields{
				s: commonStore,
			},
			args: args{
				pos: pos2 + 1,
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := tt.fields.s
			got, err := s.Read(tt.args.pos)
			if (err != nil) != tt.wantErr {
				t.Errorf("store.Read() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("store.Read() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_store_ReadAt(t *testing.T) {
	commonStore := newtestStore(t)

	record1 := []byte("hola amigo")
	_, pos1, err := commonStore.Append(record1)
	width1 := len(record1)
	l1 := make([]byte, lenWidth)
	enc.PutUint64(l1, uint64(width1))
	require.NoError(t, err)

	record2 := []byte("hola amiga!")
	_, pos2, err := commonStore.Append(record2)
	width2 := len(record2)
	l2 := make([]byte, lenWidth)
	enc.PutUint64(l2, uint64(width2))
	require.NoError(t, err)

	type fields struct {
		s *store
	}

	type args struct {
		p   []byte
		off int64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int
		wantBuf []byte
		wantErr bool
	}{
		// TODO: Add test cases.
		{
			name: "read len1",
			fields: fields{
				s: commonStore,
			},
			args: args{
				p:   make([]byte, lenWidth),
				off: int64(pos1),
			},
			want:    lenWidth,
			wantBuf: l1,
			wantErr: false,
		},
		{
			name: "read record1",
			fields: fields{
				s: commonStore,
			},
			args: args{
				p:   make([]byte, width1),
				off: int64(pos1) + lenWidth,
			},
			want:    width1,
			wantBuf: record1,
			wantErr: false,
		},
		{
			name: "read record2",
			fields: fields{
				s: commonStore,
			},
			args: args{
				p:   make([]byte, width2),
				off: int64(pos2) + lenWidth,
			},
			want:    width2,
			wantBuf: record2,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := tt.fields.s
			got, err := s.ReadAt(tt.args.p, tt.args.off)
			if (err != nil) != tt.wantErr {
				t.Errorf("store.ReadAt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("store.ReadAt() = %v, want %v", got, tt.want)
			}
			if bytes.Compare(tt.args.p, tt.wantBuf) != 0 {
				t.Errorf("store.ReadAt() = %v, want %v", tt.args.p, tt.wantBuf)
			}
		})
	}
}

/*
func Test_store_Close(t *testing.T) {
	commonStore := newtestStore(t)
	f, err := os.Open(commonStore.Name())
	require.NoError(t, err)
	f.Close()

	record1 := []byte("hola amigo")
	n, _, err := commonStore.Append(record1)
	require.NoError(t, err)

	type fields struct {
		s *store
	}

	tests := []struct {
		name    string
		fields  fields
		wantErr bool
		wantN   uint64
	}{
		// TODO: Add test cases.
		{
			name: "close",
			fields: fields{
				s: commonStore,
			},
			wantErr: false,
			wantN:   n,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := tt.fields.s
			_, err := os.Open(s.Name())
			require.NoError(t, err)
			buf, err := ioutil.ReadFile(s.File.Name())
			require.NoError(t, err)
			if err := s.Close(); (err != nil) != tt.wantErr {
				t.Errorf("store.Close() error = %v, wantErr %v", err, tt.wantErr)
			}
			//buf, err := ioutil.ReadFile(s.File.Name())
			require.NoError(t, err)
			require.Equal(t, len(buf), tt.wantN, "file bytes %v want %v", len(buf), tt.wantN)
		})
	}
}
*/
