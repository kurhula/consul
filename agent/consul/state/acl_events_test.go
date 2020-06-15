package state

import (
	"strconv"
	"strings"
	"testing"

	"github.com/hashicorp/consul/agent/consul/stream"
	"github.com/hashicorp/consul/agent/structs"
	"github.com/stretchr/testify/require"
)

func TestACLEventsFromChanges(t *testing.T) {
	cases := []struct {
		Name       string
		Setup      func(s *Store, tx *txn) error
		Mutate     func(s *Store, tx *txn) error
		WantEvents []stream.Event
	}{
		{
			Name: "token create",
			Mutate: func(s *Store, tx *txn) error {
				return s.aclTokenSetTxn(tx, tx.Index, newACLToken(1), false, false, false, false)
			},
			WantEvents: []stream.Event{
				newACLTokenEvent(100, 1),
			},
		},
		{
			Name: "token update",
			Setup: func(s *Store, tx *txn) error {
				return s.aclTokenSetTxn(tx, tx.Index, newACLToken(1), false, false, false, false)
			},
			Mutate: func(s *Store, tx *txn) error {
				// Add a policy to the token (never mind it doesn't exist for now) we
				// allow it in the set command below.
				token := newACLToken(1)
				token.Policies = []structs.ACLTokenPolicyLink{{ID: "33333333-1111-1111-1111-111111111111"}}
				return s.aclTokenSetTxn(tx, tx.Index, token, false, true, false, false)
			},
			WantEvents: []stream.Event{
				newACLTokenEvent(100, 1),
			},
		},
		{
			Name: "token delete",
			Setup: func(s *Store, tx *txn) error {
				return s.aclTokenSetTxn(tx, tx.Index, newACLToken(1), false, false, false, false)
			},
			Mutate: func(s *Store, tx *txn) error {
				token := newACLToken(1)
				return s.aclTokenDeleteTxn(tx, tx.Index, token.AccessorID, "id", nil)
			},
			WantEvents: []stream.Event{
				newACLTokenEvent(100, 1),
			},
		},
		{
			Name: "policy create",
			Mutate: func(s *Store, tx *txn) error {
				return s.aclPolicySetTxn(tx, tx.Index, newACLPolicy(1))
			},
			WantEvents: []stream.Event{
				newACLPolicyEvent(100, 1),
			},
		},
		{
			Name: "policy update",
			Setup: func(s *Store, tx *txn) error {
				return s.aclPolicySetTxn(tx, tx.Index, newACLPolicy(1))
			},
			Mutate: func(s *Store, tx *txn) error {
				policy := newACLPolicy(1)
				policy.Rules = `operator = "write"`
				return s.aclPolicySetTxn(tx, tx.Index, policy)
			},
			WantEvents: []stream.Event{
				newACLPolicyEvent(100, 1),
			},
		},
		{
			Name: "policy delete",
			Setup: func(s *Store, tx *txn) error {
				return s.aclPolicySetTxn(tx, tx.Index, newACLPolicy(1))
			},
			Mutate: func(s *Store, tx *txn) error {
				policy := newACLPolicy(1)
				return s.aclPolicyDeleteTxn(tx, tx.Index, policy.ID, s.aclPolicyGetByID, nil)
			},
			WantEvents: []stream.Event{
				newACLPolicyEvent(100, 1),
			},
		},
		{
			Name: "role create",
			Mutate: func(s *Store, tx *txn) error {
				return s.aclRoleSetTxn(tx, tx.Index, newACLRole(1, 1), true)
			},
			WantEvents: []stream.Event{
				newACLRoleEvent(100, 1),
			},
		},
		{
			Name: "role update",
			Setup: func(s *Store, tx *txn) error {
				return s.aclRoleSetTxn(tx, tx.Index, newACLRole(1, 1), true)
			},
			Mutate: func(s *Store, tx *txn) error {
				role := newACLRole(1, 1)
				policy2 := newACLPolicy(2)
				role.Policies = append(role.Policies, structs.ACLRolePolicyLink{
					ID:   policy2.ID,
					Name: policy2.Name,
				})
				return s.aclRoleSetTxn(tx, tx.Index, role, true)
			},
			WantEvents: []stream.Event{
				newACLRoleEvent(100, 1),
			},
		},
		{
			Name: "role delete",
			Setup: func(s *Store, tx *txn) error {
				return s.aclRoleSetTxn(tx, tx.Index, newACLRole(1, 1), true)
			},
			Mutate: func(s *Store, tx *txn) error {
				role := newACLRole(1, 1)
				return s.aclRoleDeleteTxn(tx, tx.Index, role.ID, s.aclRoleGetByID, nil)
			},
			WantEvents: []stream.Event{
				newACLRoleEvent(100, 1),
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			s := testStateStore(t)

			if tc.Setup != nil {
				// Bypass the publish mechanism for this test or we get into odd
				// recursive stuff...
				setupTx := s.db.WriteTxn(10)
				require.NoError(t, tc.Setup(s, setupTx))
				// Commit the underlying transaction without using wrapped Commit so we
				// avoid the whole event publishing system for setup here. It _should_
				// work but it makes debugging test hard as it will call the function
				// under test for the setup data...
				setupTx.Txn.Commit()
			}

			tx := s.db.WriteTxn(100)
			require.NoError(t, tc.Mutate(s, tx))

			// Note we call the func under test directly rather than publishChanges so
			// we can test this in isolation.
			got, err := aclEventsFromChanges(tx, tx.Changes())
			require.NoError(t, err)

			// Make sure we have the right events, only taking ordering into account
			// where it matters to account for non-determinism.
			requireEventsInCorrectPartialOrder(t, tc.WantEvents, got, func(e stream.Event) string {
				// We only care that events affecting the same actual token are ordered
				// with respect to each other so use it's ID as the key.
				switch v := e.Payload.(type) {
				case *structs.ACLToken:
					return "token:" + v.AccessorID
				case *structs.ACLPolicy:
					return "policy:" + v.ID
				case *structs.ACLRole:
					return "role:" + v.ID
				}
				return ""
			})
		})
	}
}

func newACLTokenEvent(idx uint64, n int) stream.Event {
	uuid := strings.ReplaceAll("11111111-????-????-????-????????????", "?", strconv.Itoa(n))
	return stream.Event{
		Topic: stream.Topic_ACLTokens,
		Index: idx,
		Payload: &structs.ACLToken{
			AccessorID: uuid,
			SecretID:   uuid,
		},
	}
}

func newACLPolicyEvent(idx uint64, n int) stream.Event {
	uuid := strings.ReplaceAll("22222222-????-????-????-????????????", "?", strconv.Itoa(n))
	return stream.Event{
		Topic: stream.Topic_ACLPolicies,
		Index: idx,
		Payload: &structs.ACLPolicy{
			ID: uuid,
		},
	}
}

func newACLRoleEvent(idx uint64, n int) stream.Event {
	uuid := strings.ReplaceAll("33333333-????-????-????-????????????", "?", strconv.Itoa(n))
	return stream.Event{
		Topic: stream.Topic_ACLRoles,
		Index: idx,
		Payload: &structs.ACLRole{
			ID: uuid,
		},
	}
}

func newACLToken(n int) *structs.ACLToken {
	uuid := strings.ReplaceAll("11111111-????-????-????-????????????", "?", strconv.Itoa(n))
	return &structs.ACLToken{
		AccessorID: uuid,
		SecretID:   uuid,
	}
}

func newACLPolicy(n int) *structs.ACLPolicy {
	numStr := strconv.Itoa(n)
	uuid := strings.ReplaceAll("22222222-????-????-????-????????????", "?", numStr)
	return &structs.ACLPolicy{
		ID:    uuid,
		Name:  "test_policy_" + numStr,
		Rules: `operator = "read"`,
	}
}

func newACLRole(n, p int) *structs.ACLRole {
	numStr := strconv.Itoa(n)
	uuid := strings.ReplaceAll("33333333-????-????-????-????????????", "?", numStr)
	policy := newACLPolicy(p)
	return &structs.ACLRole{
		ID:   uuid,
		Name: "test_role_" + numStr,
		Policies: []structs.ACLRolePolicyLink{{
			ID:   policy.ID,
			Name: policy.Name,
		}},
	}
}

// requireEventsInCorrectPartialOrder compares that the expected set of events
// was emitted. It allows for _independent_ events to be emitted in any order -
// this can be important because even though the transaction processing is all
// strictly ordered up until the processing func, grouping multiple updates that
// affect the same logical entity may be necessary and may impose random
// ordering changes on the eventual events if a map is used. We only care that
// events _affecting the same topic and key_ are ordered correctly with respect
// to the "expected" set of events so this helper asserts that.
//
// The caller provides a func that can return a partition key for the given
// event types and we assert that all events with the same partition key are
// deliveries in the same order. Note that this is not necessarily the same as
// topic/key since for example in Catalog only events about a specific service
// _instance_ need to be ordered while topic and key are more general.
func requireEventsInCorrectPartialOrder(
	t *testing.T,
	want, got []stream.Event,
	partKey func(stream.Event) string,
) {
	t.Helper()

	// Partion both arrays by topic/key
	wantParts := make(map[string][]stream.Event)
	gotParts := make(map[string][]stream.Event)

	for _, e := range want {
		k := partKey(e)
		wantParts[k] = append(wantParts[k], e)
	}
	for _, e := range got {
		k := partKey(e)
		gotParts[k] = append(gotParts[k], e)
	}

	for k, want := range wantParts {
		require.Equal(t, want, gotParts[k], "got incorrect events for partition: %s", k)
	}

	for k, got := range gotParts {
		if _, ok := wantParts[k]; !ok {
			require.Equal(t, nil, got, "got unwanted events for partition: %s", k)
		}
	}
}
