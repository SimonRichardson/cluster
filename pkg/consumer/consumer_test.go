package consumer

import (
	"fmt"
	"io/ioutil"
	"reflect"
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

			instance = "0.0.0.0:8080"
			id       = uuid.MustNew().Bytes()

			input = fmt.Sprintf("%s %s", string(id), uuid.MustNew().String())
		)

		peer.EXPECT().
			Current(PeerType(cluster.PeerTypeIngest)).
			Return([]string{instance}, nil).
			Times(1)
		peer.EXPECT().
			Current(PeerType(cluster.PeerTypeStore)).
			Return([]string{instance}, nil).
			Times(1)
		client.EXPECT().
			Get(URL(buildIngestNextIDPath(instance))).
			Return(response, nil).
			Times(1)
		response.EXPECT().
			Close().
			Return(nil)
		response.EXPECT().
			Bytes().
			Return(id, nil)
		client.EXPECT().
			Get(URL(buildIngestIDPath(instance, string(id)))).
			Return(response, nil).
			Times(1)
		response.EXPECT().
			Close().
			Return(nil)
		response.EXPECT().
			Reader().
			Return(ioutil.NopCloser(strings.NewReader(input)))

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

		c.guard(c.gather)

		want := []byte(input)
		if expected, actual := want, c.active.Bytes(); !reflect.DeepEqual(expected, actual) {
			t.Errorf("expected: %s, actual: %s", string(expected), string(actual))
		}
	})
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
