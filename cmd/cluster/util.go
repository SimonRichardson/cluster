package main

import (
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func interrupt(cancel <-chan struct{}) error {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-c:
		return fmt.Errorf("received signal %s", sig)
	case <-cancel:
		return errors.New("canceled")
	}
}

// "udp://host:1234", 80 => udp host:1234 host 1234
// "host:1234", 80       => tcp host:1234 host 1234
// "host", 80            => tcp host:80   host 80
func parseAddr(addr string, defaultPort int) (network, address, host string, port int, err error) {
	u, err := url.Parse(strings.ToLower(addr))
	if err != nil {
		// It's possible that we're dealing with IPv6 localhost here!
		if strings.HasPrefix(addr, "[::]") {
			var p string
			_, p, err = net.SplitHostPort(addr)
			if err != nil {
				return
			}
			if p == "" {
				p = strconv.Itoa(defaultPort)
			}
			if port, err = strconv.Atoi(p); err != nil {
				return
			}
			return "tcp", fmt.Sprintf("0.0.0.0:%d", port), "0.0.0.0", port, nil
		}
		return
	}

	switch {
	case u.Scheme == "" && u.Opaque == "" && u.Host == "" && u.Path != "": // "host"
		u.Scheme, u.Opaque, u.Host, u.Path = "tcp", "", net.JoinHostPort(u.Path, strconv.Itoa(defaultPort)), ""
	case u.Scheme != "" && u.Opaque != "" && u.Host == "" && u.Path == "": // "host:1234"
		u.Scheme, u.Opaque, u.Host, u.Path = "tcp", "", net.JoinHostPort(u.Scheme, u.Opaque), ""
	case u.Scheme != "" && u.Opaque == "" && u.Host != "" && u.Path == "": // "tcp://host[:1234]"
		if _, _, err := net.SplitHostPort(u.Host); err != nil {
			u.Host = net.JoinHostPort(u.Host, strconv.Itoa(defaultPort))
		}
	default:
		errors.Errorf("%s: unsupported address format", addr)
		return
	}

	host, portStr, err := net.SplitHostPort(u.Host)
	if err != nil {
		return
	}
	port, err = strconv.Atoi(portStr)
	if err != nil {
		return
	}

	return u.Scheme, u.Host, host, port, nil
}

func registerMetrics(mux *http.ServeMux) {
	mux.Handle("/metrics", promhttp.Handler())
}

func registerProfile(mux *http.ServeMux) {
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.Handle("/debug/pprof/block", pprof.Handler("block"))
	mux.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	mux.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	mux.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
}

func envName(name string) string {
	return strings.Replace(strings.ToUpper(name), ".", "_", -1)
}

type stringslice []string

func (ss *stringslice) Set(s string) error {
	(*ss) = append(*ss, s)
	return nil
}

func (ss *stringslice) String() string {
	if len(*ss) <= 0 {
		return "..."
	}
	return strings.Join(*ss, ", ")
}
