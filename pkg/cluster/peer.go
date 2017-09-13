package cluster

import (
	"io/ioutil"
	"net"
	"strconv"
	"time"

	"github.com/SimonRichardson/cluster/pkg/uuid"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/hashicorp/serf/serf"
	"github.com/pkg/errors"
)

const (
	defaultBroadcastTimeout         = time.Second * 10
	defaultMembersBroadcastInterval = time.Second * 5
	defaultLowMembersThreshold      = 1
)

// PeerType describes the type of peer with in the cluster.
type PeerType string

const (
	// PeerTypeStore serves the store API
	PeerTypeStore PeerType = "store"

	// PeerTypeIngest serves the ingest API
	PeerTypeIngest = "ingest"
)

// ParsePeerType parses a potential peer type and errors out if it's not a known
// valid type.
func ParsePeerType(t string) (PeerType, error) {
	switch t {
	case "store", "ingest":
		return PeerType(t), nil
	default:
		return "", errors.Errorf("invalid peer type (%s)", t)
	}

}

// Peer represents the node with in the cluster.
type Peer struct {
	members *serf.Serf
	stop    chan chan struct{}
	logger  log.Logger
}

// NewPeer creates or joins a cluster with the existing peers.
// We will listen for cluster communications on the bind addr:port.
// We advertise a PeerType HTTP API, reachable on apiPort.
func NewPeer(
	bindAddr string, bindPort int,
	advertiseAddr string, advertisePort int,
	existing []string,
	peerType PeerType,
	apiPort int,
	logger log.Logger,
) (*Peer, error) {
	level.Debug(logger).Log("bind_addr", bindAddr, "bind_port", bindPort, "ParseIP", net.ParseIP(bindAddr).String())

	config := serf.DefaultConfig()

	{
		name, err := uuid.New()
		if err != nil {
			return nil, err
		}

		config.NodeName = name.String()
		config.MemberlistConfig.BindAddr = bindAddr
		config.MemberlistConfig.BindPort = bindPort
		if advertiseAddr != "" {
			level.Debug(logger).Log("advertise_addr", advertiseAddr, "advertise_port", advertisePort)

			config.MemberlistConfig.AdvertiseAddr = advertiseAddr
			config.MemberlistConfig.AdvertisePort = advertisePort
		}
		config.LogOutput = ioutil.Discard
		config.BroadcastTimeout = defaultBroadcastTimeout
		config.Tags = encodePeerInfoTag(peerInfo{
			Type:    peerType,
			APIAddr: bindAddr,
			APIPort: bindPort,
		})
	}

	members, err := serf.Create(config)
	if err != nil {
		return nil, err
	}

	numNodes, err := members.Join(existing, true)
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

	members := p.members.Memberlist()
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
	return p.members.Memberlist().LocalNode().Name
}

// ClusterSize returns the total size of the cluster from this node's
// perspective.
func (p *Peer) ClusterSize() int {
	return p.members.Memberlist().NumMembers()
}

// State returns a JSON-serializable dump of cluster state.
// Useful for debug.
func (p *Peer) State() map[string]interface{} {
	members := p.members.Memberlist()
	return map[string]interface{}{
		"self":        members.LocalNode(),
		"members":     members.Members(),
		"num_members": members.NumMembers(),
	}
}

// Current API host:ports for the given type of node.
func (p *Peer) Current(peerType PeerType) (res []string) {
	for _, v := range p.members.Members() {
		if v.Status != serf.StatusAlive {
			continue
		}

		if info, err := decodePeerInfoTag(v.Tags); err == nil {
			var (
				matchIngest = peerType == PeerTypeIngest && info.Type == PeerTypeIngest
				matchStore  = peerType == PeerTypeStore && info.Type == PeerTypeStore
			)
			if matchIngest || matchStore {
				res = append(res, net.JoinHostPort(info.APIAddr, strconv.Itoa(info.APIPort)))
			}
		}

	}
	return
}

type peerInfo struct {
	Type    PeerType
	APIAddr string
	APIPort int
}

// encodeTagPeerInfo encodes the peer information for the node tags.
func encodePeerInfoTag(info peerInfo) map[string]string {
	return map[string]string{
		"type":     string(info.Type),
		"api_addr": info.APIAddr,
		"api_port": strconv.Itoa(info.APIPort),
	}
}

// decodePeerInfoTag gets the peer information from the node tags.
func decodePeerInfoTag(m map[string]string) (info peerInfo, err error) {
	if peerType, ok := m["type"]; ok {
		if info.Type, err = ParsePeerType(peerType); err != nil {
			return
		}
	}

	if apiPort, ok := m["api_port"]; ok {
		if info.APIPort, err = strconv.Atoi(apiPort); err != nil {
			return
		}
	}

	var ok bool
	if info.APIAddr, ok = m["api_addr"]; !ok {
		err = errors.Errorf("missing api_addr")
		return
	}

	return
}
