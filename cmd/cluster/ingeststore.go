package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"syscall"

	"github.com/pkg/errors"

	"github.com/SimonRichardson/cluster/pkg/cluster"
	"github.com/SimonRichardson/cluster/pkg/members"
	"github.com/SimonRichardson/gexec"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	defaultMembers = "real"
)

func runIngestStore(args []string) error {
	// flags for the ingeststore command
	var (
		flagset = flag.NewFlagSet("ingeststore", flag.ExitOnError)

		debug        = flagset.Bool("debug", false, "debug logging")
		apiAddr      = flagset.String("api", defaultAPIAddr, "listen address for query API")
		membersType  = flagset.String("members", defaultMembers, "real, nop")
		clusterPeers = stringslice{}
	)

	flagset.Var(&clusterPeers, "peer", "cluster peer host:port (repeatable)")

	var envArgs []string
	flagset.VisitAll(func(flag *flag.Flag) {
		key := envName(flag.Name)
		if value, ok := syscall.Getenv(key); ok {
			envArgs = append(envArgs, fmt.Sprintf("-%s=%s", flag.Name, value))
		}
	})

	flagsetArgs := append(args, envArgs...)
	flagset.Usage = usageFor(flagset, "ingeststore [flags]")
	if err := flagset.Parse(flagsetArgs); err != nil {
		return nil
	}

	// Setup the logger.
	var logger log.Logger
	{
		logLevel := level.AllowInfo()
		if *debug {
			logLevel = level.AllowAll()
		}
		logger = log.NewLogfmtLogger(os.Stdout)
		logger = log.With(logger, "ts", log.DefaultTimestampUTC)
		logger = level.NewFilter(logger, logLevel)
	}

	apiNetwork, apiAddress, _, _, err := parseAddr(*apiAddr, defaultAPIPort)
	if err != nil {
		return err
	}

	apiListener, err := net.Listen(apiNetwork, apiAddress)
	if err != nil {
		return err
	}
	level.Info(logger).Log("API", fmt.Sprintf("%s://%s", apiNetwork, apiAddress))

	// Create peer.
	var mem members.Members
	switch strings.ToLower(*membersType) {
	case "real":
		var config members.Config
		if config, err = members.Build(); err != nil {
			return err
		}

		if mem, err = members.NewRealMembers(
			config,
			log.With(logger, "component", "members"),
		); err != nil {
			return err
		}
	case "nop":
		mem = members.NewNopMembers()
	default:
		return errors.Errorf("invalid -members %q", *membersType)
	}

	peer, err := cluster.NewPeer(
		mem,
		log.With(logger, "component", "peer"),
	)
	if err != nil {
		return err
	}
	prometheus.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "oklog",
		Name:      "cluster_size",
		Help:      "Number of peers in the cluster from this node's perspective.",
	}, func() float64 { return float64(peer.ClusterSize()) }))

	// Execution group.
	var g gexec.Group
	gexec.Block(g)
	{
		// Store manages and maintains the underlying dataStore.
		g.Add(func() error {
			return nil
		}, func(error) {
		})
	}
	{
		g.Add(func() error {
			mux := http.NewServeMux()
			return http.Serve(apiListener, mux)
		}, func(error) {
			apiListener.Close()
		})
	}
	gexec.Interrupt(g)
	return g.Run()
}
