package log

import (
	"context"
	"sync"

	api "github.com/krehermann/proglog/api/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

//Replicator implements Handler interface and acts a membership handler when a
//server joins and leaves the cluster. Upon joining the cluster, it runs a loop that consumes
//from discovered peers and produces to the local server.
type Replicator struct {
	DialOpts    []grpc.DialOption
	LocalServer api.LogClient
	logger      *zap.Logger
	mu          sync.Mutex
	// servers is a map to track peers that we are replicating
	servers map[string]chan struct{}
	closed  bool
	close   chan struct{}
}

//Join Adds name,addr to list of servers to replicate, if not present, and starts go routine to replicate the data
func (r *Replicator) Join(name, addr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()
	if r.closed {
		return nil
	}
	_, ok := r.servers[name]
	if ok {
		//already replicating
		return nil
	}
	r.servers[name] = make(chan struct{})

	go r.replicate(addr, r.servers[name])

	return nil
}

//replicate connects to addr and consumes a stream of records.
//It writes each of the records to the local server of the Replicator
func (r *Replicator) replicate(addr string, leaveCh chan struct{}) {
	cc, err := grpc.Dial(addr, r.DialOpts)
	if err != nil {
		r.logError(err, "failed to dial", addr)
		return
	}
	defer cc.Close()

	client := api.NewLogClient(cc)
	ctx := context.Background()
	stream, err := client.ConsumeStream(
		ctx,
		&api.ConsumeRequest{
			Offset: 0,
		})
	if err != nil {
		r.logError(err, "failed to consume stream", addr)
		return
	}
	records := make(chan *api.Record)
	go func() {
		for {
			recv, err := stream.Recv()
			if err != nil {
				r.logError(err, "failed to recv", addr)
				return
			}
			records <- recv.Record
		}
	}()

	for {
		select {
		case <-r.close:
			return
		case <-leaveCh:
			return
		case record := <-records:
			_, err := r.LocalServer.Produce(ctx,
				&api.ProduceRequest{
					Record: record,
				})
			if err != nil {
				r.logError(err, "failed to produce", addr)
				return
			}
		}
	}
}

//Leave handles the server with the given name leaving the cluster.
//It closes the replication channel and deletes the name from the map of servers.
func (r *Replicator) Leave(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()
	_, ok := r.servers[name]
	if !ok {
		return nil
	}
	close(r.servers[name])
	delete(r.servers, name)
	return nil
}

func (r *Replicator) init() {
	if r.logger == nil {
		r.logger = zap.L().Named("replicator")
	}
	if r.servers == nil {
		r.servers = make(map[string]chan struct{})
	}
	if r.close == nil {
		r.close = make(chan struct{})
	}
}

func (r *Replicator) logError(err error, msg, addr string) {
	r.logger.Error(
		msg,
		zap.String("addr", addr),
		zap.Error(err),
	)
}

//Close closes the replicator so it doesn't replicate new servers that join
// and stops replicating existing servers
func (r *Replicator) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()
	if r.closed {
		return nil
	}
	r.closed = true
	close(r.close)
	return nil
}
