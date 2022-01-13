package server

import (
	"context"
	"io/ioutil"
	"net"
	"testing"

	api "github.com/krehermann/proglog/api/v1"
	"github.com/krehermann/proglog/internal/log"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		client api.LogClient,
		cfg *Config,
	){
		"produce/consume a message to/from log succeeds": testProduceConsume,
		"produce/consume stream succeeds":                testProduceConsumeStream,
		"consume past log boundary fails":                testConsumePastEnd,
	} {
		t.Run(scenario, func(t *testing.T) {
			client, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, client, config)
		})
	}
}

func setupTest(t *testing.T, cfgFn func(*Config)) (
	client api.LogClient,
	cfg *Config,
	teardown func(),
) {
	t.Helper()
	l, err := net.Listen("tcp", ":0")
	assert.NoError(t, err)
	clientOpts := []grpc.DialOption{grpc.WithInsecure()}
	cc, err := grpc.Dial(l.Addr().String(), clientOpts...)
	assert.NoError(t, err)

	dir, err := ioutil.TempDir("", "service-test")
	assert.NoError(t, err)

	cmtlog, err := log.NewLog(dir, log.Config{})
	assert.NoError(t, err)

	cfg = &Config{CommitLog: cmtlog}
	if cfgFn != nil {
		cfgFn(cfg)
	}
	srv, err := NewGRPCServer(cfg)
	assert.NoError(t, err)
	go func() {
		srv.Serve(l)
	}()

	client = api.NewLogClient(cc)
	return client, cfg, func() {
		srv.Stop()
		cc.Close()
		l.Close()
		cmtlog.Remove()
	}

}

func testProduceConsume(t *testing.T, client api.LogClient, cfg *Config) {
	req := &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("my name is what"),
		},
	}
	resp, err := client.Produce(context.Background(), req)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), resp.Offset)

	creq := &api.ConsumeRequest{
		Offset: resp.Offset,
	}
	cresp, err := client.Consume(context.Background(), creq)
	assert.NoError(t, err)
	assert.Equal(t, req.Record.Value, cresp.Record.Value)

}

func testConsumePastEnd(t *testing.T, client api.LogClient, cfg *Config) {
	req := &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("my name is what"),
		},
	}
	resp, err := client.Produce(context.Background(), req)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), resp.Offset)

	creq := &api.ConsumeRequest{
		Offset: resp.Offset + 1,
	}
	_, err = client.Consume(context.Background(), creq)
	got := status.Code(err) //grpc.Code(err)
	want := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	assert.Equal(t, got, want)

}

func testProduceConsumeStream(t *testing.T, client api.LogClient, cfg *Config) {
	ctx := context.Background()
	records := []*api.Record{
		{
			Value:  []byte("1"),
			Offset: 0,
		}, {
			Value:  []byte("2"),
			Offset: 1,
		}}

	{
		stream, err := client.ProduceStream(ctx)
		assert.NoError(t, err)
		for offset, record := range records {
			err := stream.Send(&api.ProduceRequest{
				Record: record,
			})
			assert.NoError(t, err)
			res, err := stream.Recv()
			assert.NoError(t, err)
			assert.Equal(t, uint64(offset), res.Offset)
		}
	}

	{
		stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
		assert.NoError(t, err)
		for _, record := range records {
			res, err := stream.Recv()
			assert.NoError(t, err)
			assert.EqualValues(t, record.Value, res.Record.Value)
			assert.EqualValues(t, record.Offset, res.Record.Offset)
		}
	}

}
