package cluster

import (
	"net"
	"strconv"
	"time"

	"github.com/SimonRichardson/cluster/pkg/members"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
)

const (
	defaultBroadcastTimeout         = time.Second * 10
	defaultMembersBroadcastInterval = time.Second * 5
	defaultLowMembersThreshold      = 1
)

const (
	// PeerTypeStore serves the store API
	PeerTypeStore members.PeerType = "store"

	// PeerTypeIngest serves the ingest API
	PeerTypeIngest = "ingest"
)

// ParsePeerType parses a potential peer type and errors out if it's not a known
// valid type.
func ParsePeerType(t string) (members.PeerType, error) {
	switch t {
	case "store", "ingest":
		return members.PeerType(t), nil
	default:
		return "", errors.Errorf("invalid peer type (%s)", t)
	}
}

// Peer represents the node with in the cluster.
type Peer struct {
	members members.Members
	stop    chan chan struct{}
	logger  log.Logger
}

// NewPeer creates or joins a cluster with the existing peers.
// We will listen for cluster communications on the bind addr:port.
// We advertise a PeerType HTTP API, reachable on apiPort.
func NewPeer(
	members members.Members,
	logger log.Logger,
) (*Peer, error) {
	numNodes, err := members.Join()
	if err != nil {
		return nil, err
	}

	level.Debug(logger).Log("joined", numNodes)

	peer := &Peer{
		members: members,
		stop:    make(chan chan struct{}),
		logger:  logger,
	}
	go peer.run()
	return peer, nil
}

func (p *Peer) run() {
	ticker := time.NewTicker(defaultMembersBroadcastInterval)
	defer ticker.Stop()

	members := p.members.MemberList()
	for {
		select {
		case <-ticker.C:
			if num := members.NumMembers(); num <= defaultLowMembersThreshold {
				level.Warn(p.logger).Log("num_members", num, "reason", "alone")
			}

		case c := <-p.stop:
			close(c)
			return
		}
	}
}

// Close out the API
func (p *Peer) Close() {
	c := make(chan struct{})
	p.stop <- c
	<-c
}

// Leave the cluster.
func (p *Peer) Leave() error {
	// Ignore this timeout for now, serf uses a config timeout.
	return p.members.Leave()
}

// Name returns unique ID of this peer in the cluster.
func (p *Peer) Name() string {
	return p.members.MemberList().LocalNode().Name()
}

// ClusterSize returns the total size of the cluster from this node's
// perspective.
func (p *Peer) ClusterSize() int {
	return p.members.MemberList().NumMembers()
}

// State returns a JSON-serializable dump of cluster state.
// Useful for debug.
func (p *Peer) State() map[string]interface{} {
	members := p.members.MemberList()
	return map[string]interface{}{
		"self":        members.LocalNode(),
		"members":     members.Members(),
		"num_members": members.NumMembers(),
	}
}

// Current API host:ports for the given type of node.
func (p *Peer) Current(peerType members.PeerType) (res []string, err error) {
	err = p.members.Walk(func(info members.PeerInfo) error {
		var (
			matchIngest = peerType == PeerTypeIngest && info.Type == PeerTypeIngest
			matchStore  = peerType == PeerTypeStore && info.Type == PeerTypeStore
		)
		if matchIngest || matchStore {
			res = append(res, net.JoinHostPort(info.APIAddr, strconv.Itoa(info.APIPort)))
		}
		return nil
	})
	return
}
