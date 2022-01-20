package discovery

import (
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/serf/serf"
	"github.com/stretchr/testify/assert"
	"github.com/travisjeffery/go-dynaport"
)

var nMembers = 3

func TestMembership(t *testing.T) {
	m, handler := setupMember(t, nil)
	m, _ = setupMember(t, m)
	m, _ = setupMember(t, m)

	assert.Eventually(t, func() bool {
		return 2 == len(handler.joins) &&
			nMembers == len(m[0].Members()) &&
			0 == len(handler.leaves)
	}, 3*time.Second, 250*time.Millisecond)

	assert.NoError(t, m[2].Leave())
	time.Sleep(3)
	//	log.Printf(" len joins %d, members %d, leaves %d", len(handler.joins), len(m[0].Members()), len(handler.leaves))

	assert.Eventually(t, func() bool {
		return 2 == len(handler.joins) &&
			nMembers == len(m[0].Members()) &&
			1 == len(handler.leaves) &&
			serf.StatusLeft == m[0].Members()[2].Status
	}, 3*time.Second, 250*time.Millisecond)

	assert.Equal(t, fmt.Sprintf("%d", 2), <-handler.leaves)
}

func setupMember(t *testing.T, members []*Membership) ([]*Membership, *handler) {
	id := len(members)
	ports := dynaport.Get(1)
	addr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
	tags := map[string]string{
		"rpc_addr": addr,
	}
	config := Config{
		NodeName: fmt.Sprintf("%d", id),
		BindAddr: addr,
		Tags:     tags,
	}
	if len(members) != 0 {
		config.StartJoinAddrs = []string{
			members[0].BindAddr,
		}
	}

	h := newHandler(nMembers * 10)
	m, err := NewMembership(h, config)
	assert.NoError(t, err)
	members = append(members, m)
	return members, h
}

type handler struct {
	joins  chan map[string]string
	leaves chan string
}

func newHandler(depth int) *handler {
	return &handler{
		joins:  make(chan map[string]string, depth),
		leaves: make(chan string, depth),
	}
}
func (h *handler) Join(name, addr string) error {
	h.joins <- map[string]string{
		"id":   name,
		"addr": addr,
	}
	return nil
}

func (h *handler) Leave(name string) error {
	h.leaves <- name
	return nil
}
