package members

import (
	"io"
	"strconv"
	"time"

	"github.com/hashicorp/serf/serf"
	"github.com/pkg/errors"
)

// PeerType describes the type of peer with in the cluster.
type PeerType string

// Config defines a configuration setup for creating a list to manage the
// members cluster
type Config struct {
	PeerType         PeerType
	NodeName         string
	BindAddr         string
	BindPort         int
	AdvertiseAddr    string
	AdvertisePort    int
	LogOutput        io.Writer
	BroadcastTimeout time.Duration
}

// Members represents a way of joining a members cluster
type Members interface {

	// Join joins an existing members cluster. Returns the number of nodes
	// successfully contacted. The returned error will be non-nil only in the
	// case that no nodes could be contacted. If ignoreOld is true, then any
	// user messages sent prior to the join will be ignored.
	Join([]string) (int, error)

	// Leave gracefully exits the cluster. It is safe to call this multiple
	// times.
	Leave() error

	// Memberlist is used to get access to the underlying Memberlist instance
	MemberList() MemberList

	// Walk over a set of alive members
	Walk(func(PeerInfo) error) error
}

// MemberList represents a way to manage members with in a cluster
type MemberList interface {

	// NumMembers returns the number of alive nodes currently known. Between
	// the time of calling this and calling Members, the number of alive nodes
	// may have changed, so this shouldn't be used to determine how many
	// members will be returned by Members.
	NumMembers() int

	// LocalNode is used to return the local Member
	LocalNode() Member

	// Members returns a point-in-time snapshot of the members of this cluster.
	Members() []Member
}

// Member represents a node in the cluster.
type Member interface {

	// Name returns the name of the member
	Name() string

	// Status is the state that a member is in.
	Status() serf.MemberStatus
}

// PeerInfo describes what each peer is, along with the addr and port of each
type PeerInfo struct {
	Type    PeerType
	APIAddr string
	APIPort int
}

// encodeTagPeerInfo encodes the peer information for the node tags.
func encodePeerInfoTag(info PeerInfo) map[string]string {
	return map[string]string{
		"type":     string(info.Type),
		"api_addr": info.APIAddr,
		"api_port": strconv.Itoa(info.APIPort),
	}
}

// decodePeerInfoTag gets the peer information from the node tags.
func decodePeerInfoTag(m map[string]string) (info PeerInfo, err error) {
	peerType, ok := m["type"]
	if !ok {
		err = errors.Errorf("missing api_addr")
		return
	}
	info.Type = PeerType(peerType)

	apiPort, ok := m["api_port"]
	if !ok {
		err = errors.Errorf("missing api_addr")
		return
	}
	if info.APIPort, err = strconv.Atoi(apiPort); err != nil {
		return
	}

	if info.APIAddr, ok = m["api_addr"]; !ok {
		err = errors.Errorf("missing api_addr")
		return
	}

	return
}
