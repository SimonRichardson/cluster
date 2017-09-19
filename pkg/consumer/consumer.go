package consumer

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/SimonRichardson/cluster/pkg/cluster"
	"github.com/SimonRichardson/cluster/pkg/ingester"
	"github.com/SimonRichardson/cluster/pkg/metrics"
	"github.com/SimonRichardson/cluster/pkg/store"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
)

const (
	defaultWaitTime = time.Second
)

// Consumer reads segments from the queue, and replicates merged segments to
// the rest of the cluster. It's implemented as a state machine: gather
// segments, replicate, commit, and repeat. All failures invalidate the entire
// batch.
type Consumer struct {
	mutex              sync.Mutex
	peer               cluster.Peer
	client             *http.Client
	segmentTargetSize  int64
	segmentTargetAge   time.Duration
	replicationFactor  int
	gatherErrors       int
	pending            map[string][]string
	active             *bytes.Buffer
	activeSince        time.Time
	stop               chan chan struct{}
	consumedSegments   metrics.Counter
	consumedBytes      metrics.Counter
	replicatedSegments metrics.Counter
	replicatedBytes    metrics.Counter
	logger             log.Logger
}

// NewConsumer creates a consumer.
func NewConsumer(
	peer cluster.Peer,
	segmentTargetSize int64,
	segmentTargetAge time.Duration,
	replicationFactor int,
	consumedSegments, consumedBytes metrics.Counter,
	replicatedSegments, replicatedBytes metrics.Counter,
	logger log.Logger,
) *Consumer {
	return &Consumer{
		mutex:              sync.Mutex{},
		peer:               peer,
		segmentTargetSize:  segmentTargetSize,
		segmentTargetAge:   segmentTargetAge,
		replicationFactor:  replicationFactor,
		gatherErrors:       0,
		pending:            map[string][]string{},
		active:             &bytes.Buffer{},
		activeSince:        time.Time{},
		stop:               make(chan chan struct{}),
		consumedSegments:   consumedSegments,
		consumedBytes:      consumedBytes,
		replicatedSegments: replicatedSegments,
		replicatedBytes:    replicatedBytes,
		logger:             logger,
	}
}

// Run consumes segments from ingest nodes, and replicates them to the cluster.
// Run returns when Stop is invoked.
func (c *Consumer) Run() {
	step := time.NewTicker(100 * time.Millisecond)
	defer step.Stop()
	state := c.gather
	for {
		select {
		case <-step.C:
			state = c.guard(state)

		case q := <-c.stop:
			c.fail()
			close(q)
			return
		}
	}
}

// Stop the consumer from consuming.
func (c *Consumer) Stop() {
	q := make(chan struct{})
	c.stop <- q
	<-q
}

// stateFn is a lazy chaining mechism, similar to a trampoline, but via
// calls through Run.
type stateFn func() stateFn

func (c *Consumer) guard(fn stateFn) stateFn {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return fn()
}

func (c *Consumer) gather() stateFn {
	var (
		base = log.With(c.logger, "state", "gather")
		warn = level.Warn(base)

		ingestInstances, err = c.peer.Current(cluster.PeerTypeIngest)
	)
	if err != nil {
		warn.Log("err", err)
		return c.gather
	}

	// A naïve way to break out of the gather loop in atypical conditions.
	if c.gatherErrors > 0 && c.gatherErrors > 2*len(ingestInstances) {
		if c.active.Len() <= 0 {
			// We didn't successfully consume any segments.
			// Nothing to do but reset and try again.
			c.gatherErrors = 0
			return c.gather
		}
		// We consumed some segment, at least.
		// Press forward to persistence.
		return c.replicate
	}

	// Zero ingest instances, wait to gather later
	if len(ingestInstances) == 0 {
		return c.gather
	}

	// Nothing to replicate too, prevent gathering until we have something to
	// replicate too.
	storeInstances, err := c.peer.Current(cluster.PeerTypeStore)
	if err != nil {
		warn.Log("err", err)
		return c.gather
	}
	if want, have := c.replicationFactor, len(storeInstances); have < want {
		warn.Log("replication_factor", want, "available_peers", have, "err", "replication currently impossible")
		time.Sleep(defaultWaitTime)

		c.gatherErrors++
		return c.gather
	}

	// More typical exit clauses.
	var (
		tooBig = int64(c.active.Len()) > c.segmentTargetSize
		tooOld = !c.activeSince.IsZero() && time.Since(c.activeSince) > c.segmentTargetAge
	)
	if tooBig || tooOld {
		return c.replicate
	}

	// Get a segment ID from a random ingester.
	ingestInstance := ingestInstances[rand.Intn(len(ingestInstances))]
	nextResp, err := c.client.Get(fmt.Sprintf("http://%s/ingest%s", ingestInstance, ingester.APIPathNext))
	if err != nil {
		warn.Log("ingester", ingestInstance, "during", ingester.APIPathNext, "err", err)
		c.gatherErrors++
		return c.gather
	}
	defer nextResp.Body.Close()
	nextRespBody, err := ioutil.ReadAll(nextResp.Body)
	if err != nil {
		warn.Log("ingester", ingestInstance, "during", ingester.APIPathNext, "err", err)
		c.gatherErrors++
		return c.gather
	}
	nextID := strings.TrimSpace(string(nextRespBody))
	if nextResp.StatusCode == http.StatusNotFound {
		// Normal, when the ingester has no more segments to give right now.
		// after enough of these errors, we should replicate
		c.gatherErrors++
		return c.gather
	}
	if nextResp.StatusCode != http.StatusOK {
		warn.Log("ingester", ingestInstance, "during", ingester.APIPathNext, "returned", nextResp.Status)
		c.gatherErrors++
		return c.gather
	}

	// Mark the segment ID as pending.
	// From this point forward, we must either commit or fail the segment.
	// If we do neither, it will eventually time out, but we should be nice.
	c.pending[ingestInstance] = append(c.pending[ingestInstance], nextID)

	// Read the segment.
	readResp, err := c.client.Get(fmt.Sprintf("http://%s/ingest%s?id=%s", ingestInstance, ingester.APIPathRead, nextID))
	if err != nil {
		// Reading failed, so we can't possibly commit the segment.
		// The simplest thing to do now is to fail everything.
		warn.Log("ingester", ingestInstance, "during", ingester.APIPathRead, "err", err)
		c.gatherErrors++
		// fail everything
		return c.fail
	}
	defer readResp.Body.Close()
	if readResp.StatusCode != http.StatusOK {
		warn.Log("ingester", ingestInstance, "during", ingester.APIPathRead, "returned", readResp.Status)
		c.gatherErrors++
		// fail everything, same as above
		return c.fail
	}

	// Merge the segment into our active segment.
	var cw countingWriter
	if _, err := mergeRecords(c.active, io.TeeReader(readResp.Body, &cw)); err != nil {
		warn.Log("ingester", ingestInstance, "during", "mergeRecords", "err", err)
		c.gatherErrors++
		// fail everything, same as above
		return c.fail
	}
	if c.activeSince.IsZero() {
		c.activeSince = time.Now()
	}

	// Repeat!
	c.consumedSegments.Inc()
	c.consumedBytes.Add(float64(cw.n))

	return c.gather
}

func (c *Consumer) replicate() stateFn {
	var (
		base = log.With(c.logger, "state", "replicate")
		warn = level.Warn(base)
	)

	// Replicate the segment to the cluster.
	peers, err := c.peer.Current(cluster.PeerTypeStore)
	if err != nil {
		warn.Log("err", err)
		return c.fail
	}

	var (
		indices    = rand.Perm(len(peers))
		replicated = 0
	)
	if want, have := c.replicationFactor, len(peers); have < want {
		warn.Log("replication_factor", want, "available_peers", have, "err", "replication currently impossible")
		return c.fail
	}

	for i := 0; i < len(indices) && replicated < c.replicationFactor; i++ {
		var (
			index    = indices[i]
			target   = peers[index]
			uri      = fmt.Sprintf("http://%s/store%s", target, store.APIPathReplicate)
			bodyType = "application/binary"
			body     = bytes.NewReader(c.active.Bytes())
		)
		resp, err := c.client.Post(uri, bodyType, body)
		if err != nil {
			warn.Log("target", target, "during", store.APIPathReplicate, "err", err)
			continue
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			warn.Log("target", target, "during", store.APIPathReplicate, "got", resp.Status)
			continue
		}
		replicated++
	}

	if replicated < c.replicationFactor {
		warn.Log("err", "failed to fully replicate", "want", c.replicationFactor, "have", replicated)
		return c.fail
	}

	// All good!
	c.replicatedSegments.Inc()
	c.replicatedBytes.Add(float64(c.active.Len()))

	return c.commit
}

func (c *Consumer) commit() stateFn {
	return c.resetVia("commit")
}

func (c *Consumer) fail() stateFn {
	return c.resetVia("failed")
}

func (c *Consumer) resetVia(commitOrFailed string) stateFn {
	var (
		base = log.With(c.logger, "state", commitOrFailed)
		warn = level.Warn(base)
	)

	// If commits fail, the segment may be re-replicated; that's OK.
	// If fails fail, the segment will eventually time-out; that's also OK.
	// So we have best-effort semantics, just log the error and move on.
	var wg sync.WaitGroup
	for instance, ids := range c.pending {
		wg.Add(len(ids))
		for _, id := range ids {
			go func(instance, id string) {
				defer wg.Done()
				uri := fmt.Sprintf("http://%s/ingest/%s?id=%s", instance, commitOrFailed, id)
				resp, err := c.client.Post(uri, "text/plain", nil)
				if err != nil {
					warn.Log("instance", instance, "during", "POST", "uri", uri, "err", err)
					return
				}
				resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					warn.Log("instance", instance, "during", "POST", "uri", uri, "status", resp.Status)
					return
				}
			}(instance, id)
		}
	}
	wg.Wait()

	// Reset various pending things.
	c.gatherErrors = 0
	c.pending = map[string][]string{}
	c.active.Reset()
	c.activeSince = time.Time{}

	// Back to the beginning.
	return c.gather
}

type countingWriter struct{ n int64 }

func (cw *countingWriter) Write(p []byte) (int, error) {
	cw.n += int64(len(p))
	return len(p), nil
}
