package main

import "testing"

func TestParseAddr(t *testing.T) {
	for _, testcase := range []struct {
		addr        string
		defaultPort int
		network     string
		address     string
		host        string
		port        int
	}{
		{"foo", 123, "tcp", "foo:123", "foo", 123},
		{"foo:80", 123, "tcp", "foo:80", "foo", 80},
		{"udp://foo", 123, "udp", "foo:123", "foo", 123},
		{"udp://foo:8080", 123, "udp", "foo:8080", "foo", 8080},
		{"tcp+dnssrv://testing:7650", 7650, "tcp+dnssrv", "testing:7650", "testing", 7650},
		{"[::]:", 123, "tcp", "0.0.0.0:123", "0.0.0.0", 123},
		{"[::]:80", 123, "tcp", "0.0.0.0:80", "0.0.0.0", 80},
	} {
		network, address, host, port, err := parseAddr(testcase.addr, testcase.defaultPort)
		if err != nil {
			t.Errorf("(%q, %d): %v", testcase.addr, testcase.defaultPort, err)
			continue
		}
		var (
			matchNetwork = network == testcase.network
			matchAddress = address == testcase.address
			matchHost    = host == testcase.host
			matchPort    = port == testcase.port
		)
		if !matchNetwork || !matchAddress || !matchHost || !matchPort {
			t.Errorf("(%q, %d): want [%s %s %s %d], have [%s %s %s %d]",
				testcase.addr, testcase.defaultPort,
				testcase.network, testcase.address, testcase.host, testcase.port,
				network, address, host, port,
			)
			continue
		}
	}
}

func TestEnvName(t *testing.T) {
	for _, testcase := range []struct {
		value string
		want  string
	}{
		{"name", "NAME"},
		{"name.subname", "NAME_SUBNAME"},
		{"name..SubName", "NAME__SUBNAME"},
		{".NAmE.", "_NAME_"},
	} {
		t.Run(testcase.value, func(t *testing.T) {
			if expected, actual := testcase.want, envName(testcase.value); expected != actual {
				t.Errorf("expected: %s, actual: %s", expected, actual)
			}
		})
	}
}
