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
	"github.com/pkg/errors"
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

	t.Run("gather with peer current ingest failure", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		var (
			peer               = clusterMocks.NewMockPeer(ctrl)
			consumedSegments   = metricMocks.NewMockCounter(ctrl)
			consumedBytes      = metricMocks.NewMockCounter(ctrl)
			replicatedSegments = metricMocks.NewMockCounter(ctrl)
			replicatedBytes    = metricMocks.NewMockCounter(ctrl)

			client = clientsMocks.NewMockClient(ctrl)
		)

		peer.EXPECT().
			Current(PeerType(cluster.PeerTypeIngest)).
			Return(nil, errors.New("bad"))

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
	})

	t.Run("gather with gatherErrors and no activity", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		var (
			peer               = clusterMocks.NewMockPeer(ctrl)
			consumedSegments   = metricMocks.NewMockCounter(ctrl)
			consumedBytes      = metricMocks.NewMockCounter(ctrl)
			replicatedSegments = metricMocks.NewMockCounter(ctrl)
			replicatedBytes    = metricMocks.NewMockCounter(ctrl)

			client = clientsMocks.NewMockClient(ctrl)

			instance  = "0.0.0.0:8080"
			instances = []string{instance}
		)

		expectPeerType(peer, instances, cluster.PeerTypeIngest)

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

		c.gatherErrors = 10

		got := c.guard(c.gather)
		if expected, actual := c.gather, got; !stateFnEqual(expected, actual) {
			t.Errorf("expected: %T, actual: %T", expected, actual)
		}
	})

	t.Run("gather with gatherErrors and some activity", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		var (
			peer               = clusterMocks.NewMockPeer(ctrl)
			consumedSegments   = metricMocks.NewMockCounter(ctrl)
			consumedBytes      = metricMocks.NewMockCounter(ctrl)
			replicatedSegments = metricMocks.NewMockCounter(ctrl)
			replicatedBytes    = metricMocks.NewMockCounter(ctrl)

			client = clientsMocks.NewMockClient(ctrl)

			instance  = "0.0.0.0:8080"
			instances = []string{instance}

			id = uuid.MustNew().Bytes()

			input = fmt.Sprintf("%s %s", string(id), uuid.MustNew().String())
		)

		expectPeerType(peer, instances, cluster.PeerTypeIngest)

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

		c.gatherErrors = 10
		c.active.WriteString(input)

		got := c.guard(c.gather)
		if expected, actual := c.replicate, got; !stateFnEqual(expected, actual) {
			t.Errorf("expected: %T, actual: %T", expected, actual)
		}
	})

	t.Run("gather with no instances", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		var (
			peer               = clusterMocks.NewMockPeer(ctrl)
			consumedSegments   = metricMocks.NewMockCounter(ctrl)
			consumedBytes      = metricMocks.NewMockCounter(ctrl)
			replicatedSegments = metricMocks.NewMockCounter(ctrl)
			replicatedBytes    = metricMocks.NewMockCounter(ctrl)

			client = clientsMocks.NewMockClient(ctrl)

			instances = []string{}
		)

		expectPeerType(peer, instances, cluster.PeerTypeIngest)

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
	})

	t.Run("gather with peer current store failure", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		var (
			peer               = clusterMocks.NewMockPeer(ctrl)
			consumedSegments   = metricMocks.NewMockCounter(ctrl)
			consumedBytes      = metricMocks.NewMockCounter(ctrl)
			replicatedSegments = metricMocks.NewMockCounter(ctrl)
			replicatedBytes    = metricMocks.NewMockCounter(ctrl)

			client = clientsMocks.NewMockClient(ctrl)

			instance  = "0.0.0.0:8080"
			instances = []string{instance}
		)

		expectPeerType(peer, instances, cluster.PeerTypeIngest)
		peer.EXPECT().
			Current(PeerType(cluster.PeerTypeStore)).
			Return(nil, errors.New("bad")).Times(1)

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
	})

	t.Run("gather with replication factor failure", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		var (
			peer               = clusterMocks.NewMockPeer(ctrl)
			consumedSegments   = metricMocks.NewMockCounter(ctrl)
			consumedBytes      = metricMocks.NewMockCounter(ctrl)
			replicatedSegments = metricMocks.NewMockCounter(ctrl)
			replicatedBytes    = metricMocks.NewMockCounter(ctrl)

			client = clientsMocks.NewMockClient(ctrl)

			instance  = "0.0.0.0:8080"
			instances = []string{instance}
		)

		expectPeerType(peer, instances, cluster.PeerTypeIngest)
		expectPeerType(peer, instances, cluster.PeerTypeStore)

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

		c.replicationFactor = len(instance) + 1
		c.gatherWaitTime = 0

		if expected, actual := 0, c.gatherErrors; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}

		got := c.guard(c.gather)
		if expected, actual := c.gather, got; !stateFnEqual(expected, actual) {
			t.Errorf("expected: %T, actual: %T", expected, actual)
		}
		if expected, actual := 1, c.gatherErrors; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})

	t.Run("gather with existing content", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		var (
			peer               = clusterMocks.NewMockPeer(ctrl)
			consumedSegments   = metricMocks.NewMockCounter(ctrl)
			consumedBytes      = metricMocks.NewMockCounter(ctrl)
			replicatedSegments = metricMocks.NewMockCounter(ctrl)
			replicatedBytes    = metricMocks.NewMockCounter(ctrl)

			client = clientsMocks.NewMockClient(ctrl)

			instance  = "0.0.0.0:8080"
			instances = []string{instance}

			id    = uuid.MustNew().Bytes()
			input = fmt.Sprintf("%s %s", string(id), uuid.MustNew().String())
		)

		expectPeerType(peer, instances, cluster.PeerTypeIngest)
		expectPeerType(peer, instances, cluster.PeerTypeStore)

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

		c.segmentTargetSize = 1
		c.active.WriteString(input)

		got := c.guard(c.gather)
		if expected, actual := c.replicate, got; !stateFnEqual(expected, actual) {
			t.Errorf("expected: %T, actual: %T", expected, actual)
		}
		if expected, actual := 0, c.gatherErrors; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})

	t.Run("gather with client failure for next id", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		var (
			peer               = clusterMocks.NewMockPeer(ctrl)
			consumedSegments   = metricMocks.NewMockCounter(ctrl)
			consumedBytes      = metricMocks.NewMockCounter(ctrl)
			replicatedSegments = metricMocks.NewMockCounter(ctrl)
			replicatedBytes    = metricMocks.NewMockCounter(ctrl)

			client = clientsMocks.NewMockClient(ctrl)

			instance  = "0.0.0.0:8080"
			instances = []string{instance}
		)

		expectPeerType(peer, instances, cluster.PeerTypeIngest)
		expectPeerType(peer, instances, cluster.PeerTypeStore)

		client.EXPECT().
			Get(URL(buildIngestNextIDPath(instance))).
			Return(nil, errors.New("bad")).Times(1)

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
		if expected, actual := 1, c.gatherErrors; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})

	t.Run("gather with client response failure for next id", func(t *testing.T) {
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
		)

		expectPeerType(peer, instances, cluster.PeerTypeIngest)
		expectPeerType(peer, instances, cluster.PeerTypeStore)

		expectClientGet(client, response, buildIngestNextIDPath(instance))
		response.EXPECT().
			Bytes().
			Return(nil, errors.New("bad"))

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
		if expected, actual := 1, c.gatherErrors; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})

	t.Run("gather with client failure for next", func(t *testing.T) {
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

			id = uuid.MustNew().Bytes()
		)

		expectPeerType(peer, instances, cluster.PeerTypeIngest)
		expectPeerType(peer, instances, cluster.PeerTypeStore)

		expectClientGetBytes(
			client,
			response,
			buildIngestNextIDPath(instance),
			id,
		)
		client.EXPECT().
			Get(URL(buildIngestIDPath(instance, string(id)))).
			Return(nil, errors.New("bad")).Times(1)

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
		if expected, actual := c.fail, got; !stateFnEqual(expected, actual) {
			t.Errorf("expected: %T, actual: %T", expected, actual)
		}
		if expected, actual := 1, c.gatherErrors; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})

	t.Run("gather with client response failure for next", func(t *testing.T) {
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

			id = uuid.MustNew().Bytes()
		)

		expectPeerType(peer, instances, cluster.PeerTypeIngest)
		expectPeerType(peer, instances, cluster.PeerTypeStore)

		expectClientGetBytes(
			client,
			response,
			buildIngestNextIDPath(instance),
			id,
		)
		expectClientGet(client, response, buildIngestIDPath(instance, string(id)))
		response.EXPECT().
			Reader().
			Return(ioutil.NopCloser(strings.NewReader("\n\n")))

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
		if expected, actual := c.fail, got; !stateFnEqual(expected, actual) {
			t.Errorf("expected: %T, actual: %T", expected, actual)
		}
		if expected, actual := 1, c.gatherErrors; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})

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

			id    = uuid.MustNew().Bytes()
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
		if expected, actual := 0, c.gatherErrors; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
		if expected, actual := string(id), c.pending[instance][0]; expected != actual {
			t.Errorf("expected: %s, actual: %s", expected, actual)
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
