package cluster

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/SimonRichardson/cluster/pkg/members"
	"github.com/SimonRichardson/cluster/pkg/members/mocks"
	"github.com/go-kit/kit/log"
	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
)

func TestPeerType(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name          string
		input, output string
		valid         bool
	}{
		{"store",
			"store", "store",
			true,
		},
		{"ingest",
			"ingest", "ingest",
			true,
		},
		{"bad",
			"bad", "",
			false,
		},
	}

	for _, v := range testCases {
		t.Run(v.name, func(t *testing.T) {
			peerType, err := ParsePeerType(v.input)
			if err != nil && v.valid {
				t.Fatal(err)
			}
			if expected, actual := v.output, string(peerType); expected != actual {
				t.Fatalf("expected %q, actual %q", expected, actual)
			}
		})
	}
}

func TestPeer(t *testing.T) {
	t.Parallel()

	t.Run("join", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		var (
			members    = mocks.NewMockMembers(ctrl)
			memberlist = mocks.NewMockMemberList(ctrl)
		)

		members.EXPECT().
			Join().
			Return(1, nil).
			Times(1)
		members.EXPECT().
			MemberList().
			Return(memberlist).
			Times(1)

		p := NewPeer(members, log.NewNopLogger())
		n, err := p.Join()
		defer p.Close()

		if expected, actual := 1, n; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
		if expected, actual := true, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("join with failure", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		var (
			members = mocks.NewMockMembers(ctrl)
		)

		members.EXPECT().
			Join().
			Return(0, errors.New("bad")).
			Times(1)

		p := NewPeer(members, log.NewNopLogger())
		_, err := p.Join()

		if expected, actual := false, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("leave", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		members := mocks.NewMockMembers(ctrl)

		members.EXPECT().
			Leave().
			Return(nil).
			Times(1)

		p := NewPeer(members, log.NewNopLogger())
		err := p.Leave()

		if expected, actual := true, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("name", func(t *testing.T) {
		fn := func(name string) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			var (
				members    = mocks.NewMockMembers(ctrl)
				memberlist = mocks.NewMockMemberList(ctrl)
				member     = mocks.NewMockMember(ctrl)
			)

			members.EXPECT().
				MemberList().
				Return(memberlist).
				Times(1)
			memberlist.EXPECT().
				LocalNode().
				Return(member).
				Times(1)
			member.EXPECT().
				Name().
				Return(name).
				Times(1)

			p := NewPeer(members, log.NewNopLogger())
			return p.Name() == name
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("cluster size", func(t *testing.T) {
		fn := func(size int) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			var (
				members    = mocks.NewMockMembers(ctrl)
				memberlist = mocks.NewMockMemberList(ctrl)
			)

			members.EXPECT().
				MemberList().
				Return(memberlist).
				Times(1)
			memberlist.EXPECT().
				NumMembers().
				Return(size).
				Times(1)

			p := NewPeer(members, log.NewNopLogger())
			return p.ClusterSize() == size
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("state", func(t *testing.T) {
		fn := func(name string, memberNames []string, size int) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			var (
				m = make([]members.Member, len(memberNames))

				members    = mocks.NewMockMembers(ctrl)
				memberlist = mocks.NewMockMemberList(ctrl)
				member     = mocks.NewMockMember(ctrl)
			)

			for k, v := range memberNames {
				n := mocks.NewMockMember(ctrl)
				n.EXPECT().Name().Return(v).Times(1)

				m[k] = n
			}

			members.EXPECT().
				MemberList().
				Return(memberlist).
				Times(1)
			memberlist.EXPECT().
				NumMembers().
				Return(size).
				Times(1)
			memberlist.EXPECT().
				LocalNode().
				Return(member).
				Times(1)
			memberlist.EXPECT().
				Members().
				Return(m).
				Times(1)
			member.EXPECT().
				Name().
				Return(name).
				Times(1)

			p := NewPeer(members, log.NewNopLogger())

			want := map[string]interface{}{
				"self":        name,
				"members":     memberNames,
				"num_members": size,
			}
			return reflect.DeepEqual(p.State(), want)
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("current", func(t *testing.T) {
		fn := func(hosts []ASCII) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			hostStrings := unwrapASCII(hosts)

			members := mocks.NewMockMembers(ctrl)
			members.EXPECT().
				Walk(Func(hostStrings)).
				Return(nil).
				Times(1)

			p := NewPeer(members, log.NewNopLogger())
			got, err := p.Current(PeerTypeIngest)

			if expected, actual := true, err == nil; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}

			want := make([]string, len(hostStrings))
			for k, v := range hostStrings {
				want[k] = fmt.Sprintf("%s:%d", v, 8080)
			}

			return (len(want) == 0 && len(got) == 0) ||
				reflect.DeepEqual(want, got)
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

// ASCII creates a series of tags that are ascii compliant.
type ASCII []byte

// Generate allows ASCII to be used within quickcheck scenarios.
func (ASCII) Generate(r *rand.Rand, size int) reflect.Value {
	var (
		chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
		res   = make([]byte, size)
	)

	for k := range res {
		res[k] = byte(chars[r.Intn(len(chars)-1)])
	}

	return reflect.ValueOf(res)
}

func (a ASCII) Slice() []byte {
	return a
}

func (a ASCII) String() string {
	return string(a)
}

func unwrapASCII(a []ASCII) []string {
	res := make([]string, len(a))
	for k, v := range a {
		res[k] = v.String()
	}
	return res
}

type funcMatcher struct {
	hosts []string
}

func (m funcMatcher) Matches(x interface{}) bool {
	if fn, ok := x.(func(members.PeerInfo) error); ok {
		for _, v := range m.hosts {
			if err := fn(members.PeerInfo{
				Type:    PeerTypeIngest,
				APIAddr: v,
				APIPort: 8080,
			}); err != nil {
				panic(err)
			}
		}
		return true
	}
	return false
}

func (funcMatcher) String() string {
	return "is func"
}

func Func(hosts []string) gomock.Matcher { return funcMatcher{hosts} }
