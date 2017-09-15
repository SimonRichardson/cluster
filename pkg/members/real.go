package members

import (
	"github.com/SimonRichardson/cluster/pkg/uuid"
	"github.com/go-kit/kit/log"
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/serf/serf"
)

type realMembers struct {
	members *serf.Serf
	logger  log.Logger
}

// New creates a new members list to join.
func New(config Config, logger log.Logger) (Members, error) {
	c, err := transformConfig(config)
	if err != nil {
		return nil, err
	}

	members, err := serf.Create(c)
	if err != nil {
		return nil, err
	}

	return &realMembers{members, logger}, nil
}

func (r *realMembers) Join(existing []string) (int, error) {
	return r.members.Join(existing, true)
}

func (r *realMembers) Leave() error {
	return r.members.Leave()
}

func (r *realMembers) MemberList() MemberList {
	return &realMemberList{
		r.members.Memberlist(),
		r.logger,
	}
}

func (r *realMembers) Walk(fn func(PeerInfo) error) error {
	for _, v := range r.members.Members() {
		if v.Status != serf.StatusAlive {
			continue
		}

		if info, err := decodePeerInfoTag(v.Tags); err == nil {
			if e := fn(info); e != nil {
				return err
			}
		}
	}
	return nil
}

type realMemberList struct {
	list   *memberlist.Memberlist
	logger log.Logger
}

func (r *realMemberList) NumMembers() int {
	return r.list.NumMembers()
}

func (r *realMemberList) LocalNode() Member {
	return &realMember{r.list.LocalNode()}
}

func (r *realMemberList) Members() []Member {
	m := r.list.Members()
	n := make([]Member, len(m))
	for k, v := range m {
		n[k] = &realMember{v}
	}
	return n
}

type realMember struct {
	member *memberlist.Node
}

func (r *realMember) Name() string {
	return r.member.Name
}

func (r *realMember) Status() serf.MemberStatus {
	return 0
}

func transformConfig(config Config) (*serf.Config, error) {
	c := serf.DefaultConfig()

	name, err := uuid.New()
	if err != nil {
		return nil, err
	}

	c.NodeName = name.String()
	c.MemberlistConfig.BindAddr = config.BindAddr
	c.MemberlistConfig.BindPort = config.BindPort
	if config.AdvertiseAddr != "" {
		c.MemberlistConfig.AdvertiseAddr = config.AdvertiseAddr
		c.MemberlistConfig.AdvertisePort = config.AdvertisePort
	}
	c.LogOutput = config.LogOutput
	c.BroadcastTimeout = config.BroadcastTimeout
	c.Tags = encodePeerInfoTag(PeerInfo{
		Type:    config.PeerType,
		APIAddr: config.BindAddr,
		APIPort: config.BindPort,
	})

	return c, nil
}
