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
	LogOuput         io.Writer
	BroadcastTimeout time.Duration
	Tags             map[string]string
}

type List interface {
	Create(Config) (Members, error)
}

type Members interface {
	Join([]string)
	List() MembersList
}

type MembersList interface {
	NumMembers() int
	Leave() error
	LocalNode() Node
	Members() []Node
}

type Node interface {
	Name() string
	Status() serf.MemberStatus
}
