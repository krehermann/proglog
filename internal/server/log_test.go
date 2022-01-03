package server

import (
	"reflect"
	"sync"
	"testing"
)

func TestLog_Append(t *testing.T) {
	type fields struct {
		mu      sync.Mutex
		records []Record
	}
	type args struct {
		r Record
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    uint64
		wantErr bool
	}{
		{
			name:    "append",
			fields:  fields{mu: sync.Mutex{}, records: make([]Record, 0)},
			args:    args{r: Record{Value: []byte("a record")}},
			want:    uint64(0),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &Log{
				mu:      tt.fields.mu,
				records: tt.fields.records,
			}
			got, err := l.Append(tt.args.r)
			if (err != nil) != tt.wantErr {
				t.Errorf("Log.Append() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Log.Append() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLog_Read(t *testing.T) {
	type fields struct {
		mu      sync.Mutex
		records []Record
	}
	type args struct {
		offset uint64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    Record
		wantErr bool
	}{
		{
			name:    "read empty",
			fields:  fields{mu: sync.Mutex{}, records: make([]Record, 0)},
			args:    args{offset: uint64(0)},
			want:    Record{},
			wantErr: true,
		},
		{
			name:    "read one",
			fields:  fields{mu: sync.Mutex{}, records: make([]Record, 1)},
			args:    args{offset: uint64(0)},
			want:    Record{},
			wantErr: false,
		},
		{
			name:    "out of bounds",
			fields:  fields{mu: sync.Mutex{}, records: make([]Record, 1)},
			args:    args{offset: uint64(2)},
			want:    Record{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &Log{
				mu:      tt.fields.mu,
				records: tt.fields.records,
			}
			got, err := l.Read(tt.args.offset)
			if (err != nil) != tt.wantErr {
				t.Errorf("Log.Read() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Log.Read() = %v, want %v", got, tt.want)
			}
		})
	}
}
