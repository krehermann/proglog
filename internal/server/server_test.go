package server

import (
	"context"
	"io/ioutil"
	"net"
	"testing"

	api "github.com/krehermann/proglog/api/v1"
	"github.com/krehermann/proglog/internal/auth"
	"github.com/krehermann/proglog/internal/config"
	"github.com/krehermann/proglog/internal/log"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		rootClient api.LogClient,
		nobodyClient api.LogClient,
		cfg *Config,
	){
		"produce/consume a message to/from log succeeds": testProduceConsume,
		"produce/consume stream succeeds":                testProduceConsumeStream,
		"consume past log boundary fails":                testConsumePastEnd,
		"unauthorized fails":                             testUnathorized,
	} {
		t.Run(scenario, func(t *testing.T) {
			rootClient, nobodyClient, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, rootClient, nobodyClient, config)
		})
	}
}

func setupTest(t *testing.T, cfgFn func(*Config)) (
	rootClient api.LogClient,
	nobodyClient api.LogClient,
	cfg *Config,
	teardown func(),
) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)

	newClient := func(crtPath, keyPath string) (
		*grpc.ClientConn,
		api.LogClient,
		[]grpc.DialOption,
	) {
		clientTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
			CertFile: crtPath,
			KeyFile:  keyPath,
			CAFile:   config.CAFile,
			Server:   false,
		})
		assert.NoError(t, err)
		clientCreds := credentials.NewTLS(clientTLSConfig)

		opts := []grpc.DialOption{grpc.WithTransportCredentials(clientCreds)}
		cc, err := grpc.Dial(l.Addr().String(),
			opts...)
		assert.NoError(t, err)
		client := api.NewLogClient(cc)
		return cc, client, opts
	}
	var rootConn *grpc.ClientConn
	rootConn, rootClient, _ = newClient(
		config.RootClientCertFile,
		config.RootClientKeyFile,
	)
	var nobodyConn *grpc.ClientConn
	nobodyConn, nobodyClient, _ = newClient(
		config.NobodyClientCertFile,
		config.NobodyClientKeyFile,
	)
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: l.Addr().String(),
		Server:        true,
	})
	assert.NoError(t, err)
	serverCreds := credentials.NewTLS(serverTLSConfig)
	dir, err := ioutil.TempDir("", "service-test")
	assert.NoError(t, err)

	cmtlog, err := log.NewLog(dir, log.Config{})
	assert.NoError(t, err)

	authorizer := auth.New(config.ACLModelFile, config.ACLPolicyFile)
	cfg = &Config{CommitLog: cmtlog, Authorizer: authorizer}
	if cfgFn != nil {
		cfgFn(cfg)
	}
	srv, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
	assert.NoError(t, err)
	go func() {
		srv.Serve(l)
	}()

	return rootClient, nobodyClient, cfg, func() {
		srv.Stop()
		rootConn.Close()
		nobodyConn.Close()
		l.Close()
		//cmtlog.Remove()
	}

}

func testProduceConsume(t *testing.T, client, _ api.LogClient, cfg *Config) {
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

func testConsumePastEnd(t *testing.T, client, _ api.LogClient, cfg *Config) {
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

func testProduceConsumeStream(t *testing.T, client, _ api.LogClient, cfg *Config) {
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

func testUnathorized(
	t *testing.T,
	_,
	client api.LogClient,
	config *Config,
) {
	ctx := context.Background()
	produce, err := client.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("stand out of our light"),
		},
	})
	assert.Nil(t, produce)

	gotCode, wantCode := status.Code(err), codes.PermissionDenied
	assert.Equal(t, wantCode, gotCode)

	consume, err := client.Consume(ctx,
		&api.ConsumeRequest{
			Offset: 0,
		})
	assert.Nil(t, consume)
	gotCode, wantCode = status.Code(err), codes.PermissionDenied
	assert.Equal(t, wantCode, gotCode)
}
