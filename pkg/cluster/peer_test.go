package cluster

import "testing"

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
