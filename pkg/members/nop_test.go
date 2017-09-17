package members

import "testing"

func TestNopMembers(t *testing.T) {
	t.Parallel()

	t.Run("join", func(t *testing.T) {
		members, err := NewNopMembers()
		if err != nil {
			t.Fatal(err)
		}

		x, err := members.Join()
		if err != nil {
			t.Fatal(err)
		}

		if expected, actual := 0, x; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})

	t.Run("leave", func(t *testing.T) {
		members, err := NewNopMembers()
		if err != nil {
			t.Fatal(err)
		}

		err = members.Leave()
		if expected, actual := true, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("member list", func(t *testing.T) {
		members, err := NewNopMembers()
		if err != nil {
			t.Fatal(err)
		}

		list := members.MemberList()
		if expected, actual := false, list == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("walk", func(t *testing.T) {
		members, err := NewNopMembers()
		if err != nil {
			t.Fatal(err)
		}

		err = members.Walk(func(PeerInfo) error {
			t.Fatal("failed if called")
			return nil
		})
		if expected, actual := true, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("close", func(t *testing.T) {
		members, err := NewNopMembers()
		if err != nil {
			t.Fatal(err)
		}

		err = members.Close()
		if expected, actual := true, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})
}

func TestNopMemberList(t *testing.T) {
	t.Parallel()

	t.Run("number of members", func(t *testing.T) {
		members, err := NewNopMembers()
		if err != nil {
			t.Fatal(err)
		}

		amount := members.MemberList().NumMembers()
		if expected, actual := 0, amount; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})

	t.Run("local node", func(t *testing.T) {
		members, err := NewNopMembers()
		if err != nil {
			t.Fatal(err)
		}

		node := members.MemberList().LocalNode()
		if expected, actual := false, node == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("local node name", func(t *testing.T) {
		members, err := NewNopMembers()
		if err != nil {
			t.Fatal(err)
		}

		name := members.MemberList().LocalNode().Name()
		if expected, actual := "", name; expected != actual {
			t.Errorf("expected: %s, actual: %s", expected, actual)
		}
	})

	t.Run("members", func(t *testing.T) {
		members, err := NewNopMembers()
		if err != nil {
			t.Fatal(err)
		}

		m := members.MemberList().Members()
		if expected, actual := 0, len(m); expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})
}
