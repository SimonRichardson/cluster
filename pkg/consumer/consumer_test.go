package consumer

import (
	"fmt"
	"io"
	"io/ioutil"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	clientsMocks "github.com/SimonRichardson/cluster/pkg/clients/mocks"
	"github.com/SimonRichardson/cluster/pkg/cluster"
	clusterMocks "github.com/SimonRichardson/cluster/pkg/cluster/mocks"
	"github.com/SimonRichardson/cluster/pkg/members"
	metricMocks "github.com/SimonRichardson/cluster/pkg/metrics/mocks"
	"github.com/SimonRichardson/cluster/pkg/uuid"
	"github.com/go-kit/kit/log"
	"github.com/golang/mock/gomock"
)

func TestConsumer(t *testing.T) {
	t.Parallel()

	expectPeerType := func(p *clusterMocks.MockPeer,
		res []string,
		t members.PeerType,
	) {
		p.EXPECT().
			Current(PeerType(t)).
			Return(res, nil).Times(1)
	}
	expectClientGet := func(c *clientsMocks.MockClient,
		r *clientsMocks.MockResponse,
		u string,
	) {
		c.EXPECT().
			Get(URL(u)).
			Return(r, nil).Times(1)
		r.EXPECT().
			Close().
			Return(nil)
	}
	expectClientGetBytes := func(c *clientsMocks.MockClient,
		r *clientsMocks.MockResponse,
		u string,
		res []byte,
	) {
		expectClientGet(c, r, u)
		r.EXPECT().
			Bytes().
			Return(res, nil)
	}
	expectClientGetReader := func(c *clientsMocks.MockClient,
		r *clientsMocks.MockResponse,
		u string,
		res io.ReadCloser,
	) {
		expectClientGet(c, r, u)
		r.EXPECT().
			Reader().
			Return(res)
	}

	t.Run("gather", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		var (
			peer               = clusterMocks.NewMockPeer(ctrl)
			consumedSegments   = metricMocks.NewMockCounter(ctrl)
			consumedBytes      = metricMocks.NewMockCounter(ctrl)
			replicatedSegments = metricMocks.NewMockCounter(ctrl)
			replicatedBytes    = metricMocks.NewMockCounter(ctrl)

			client   = clientsMocks.NewMockClient(ctrl)
			response = clientsMocks.NewMockResponse(ctrl)

			instance  = "0.0.0.0:8080"
			instances = []string{instance}
			id        = uuid.MustNew().Bytes()

			input = fmt.Sprintf("%s %s", string(id), uuid.MustNew().String())
		)

		expectPeerType(peer, instances, cluster.PeerTypeIngest)
		expectPeerType(peer, instances, cluster.PeerTypeStore)

		expectClientGetBytes(
			client,
			response,
			buildIngestNextIDPath(instance),
			id,
		)
		expectClientGetReader(
			client,
			response,
			buildIngestIDPath(instance, string(id)),
			ioutil.NopCloser(strings.NewReader(input)),
		)

		consumedSegments.EXPECT().Inc()
		consumedBytes.EXPECT().Add(float64(len(input)))

		c := NewConsumer(
			peer,
			client,
			100,
			time.Minute,
			1,
			consumedSegments, consumedBytes,
			replicatedSegments, replicatedBytes,
			log.NewNopLogger(),
		)

		got := c.guard(c.gather)
		if expected, actual := c.gather, got; !stateFnEqual(expected, actual) {
			t.Errorf("expected: %T, actual: %T", expected, actual)
		}

		want := []byte(input)
		if expected, actual := want, c.active.Bytes(); !reflect.DeepEqual(expected, actual) {
			t.Errorf("expected: %s, actual: %s", string(expected), string(actual))
		}
		if expected, actual := time.Now().Add(-time.Millisecond), c.activeSince; !actual.After(expected) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})
}

func stateFnEqual(a, b stateFn) bool {
	var (
		x = runtime.FuncForPC(reflect.ValueOf(a).Pointer()).Name()
		y = runtime.FuncForPC(reflect.ValueOf(b).Pointer()).Name()
	)
	return x == y
}

type peerTypeMatcher struct {
	peerType members.PeerType
}

func (m peerTypeMatcher) Matches(x interface{}) bool {
	if p, ok := x.(members.PeerType); ok {
		return p.String() == m.peerType.String()
	}
	return false
}

func (peerTypeMatcher) String() string {
	return "is peer type"
}

func PeerType(p members.PeerType) gomock.Matcher { return peerTypeMatcher{p} }

type urlMatcher struct {
	url string
}

func (m urlMatcher) Matches(x interface{}) bool {
	if p, ok := x.(string); ok {
		return p == m.url
	}
	return false
}

func (m urlMatcher) String() string {
	return fmt.Sprintf("%s is url", m.url)
}

func URL(p string) gomock.Matcher { return urlMatcher{p} }
