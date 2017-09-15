package members

import (
	"io"
	"time"

	"github.com/hashicorp/serf/serf"
)

type Config struct {
	NodeName         string
	BindAddr         string
	BindPort         int
	AdvertiseAddr    string
	AdvertisePort    string
	LogOutput        io.Writer
	BroadcastTimeout time.Duration
	Tags             map[string]string
}

type List interface {
	// Create creates a new members instance, starting all the background tasks
	// to maintain cluster membership information.
	Create(Config) (Members, error)
}

type Members interface {
	// Join joins an existing members cluster. Returns the number of nodes
	// successfully contacted. The returned error will be non-nil only in the
	// case that no nodes could be contacted. If ignoreOld is true, then any
	// user messages sent prior to the join will be ignored.
	Join([]string)

	// Memberlist is used to get access to the underlying Memberlist instance
	MemberList() MemberList
}

type MemberList interface {

	// NumMembers returns the number of alive nodes currently known. Between
	// the time of calling this and calling Members, the number of alive nodes
	// may have changed, so this shouldn't be used to determine how many
	// members will be returned by Members.
	NumMembers() int

	// Leave gracefully exits the cluster. It is safe to call this multiple
	// times.
	Leave() error

	// LocalNode is used to return the local Node
	LocalNode() Node

	// Members returns a point-in-time snapshot of the members of this cluster.
	Members() []Node
}

type Node interface {
	Name() string
	Status() serf.MemberStatus
}
