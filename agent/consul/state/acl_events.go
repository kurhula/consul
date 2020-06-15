package state

import (
	"github.com/hashicorp/consul/agent/consul/stream"
	"github.com/hashicorp/consul/agent/structs"
	memdb "github.com/hashicorp/go-memdb"
)

// ACLEventsFromChanges returns all the ACL token, policy or role events that
// should be emitted given a set of changes to the state store.
// TODO: unmethod
// TODO: Add OpDelete/OpUpdate to the event or payload?
func aclEventsFromChanges(tx *txn, changes memdb.Changes) ([]stream.Event, error) {
	var events []stream.Event

	// TODO: make this a method on memDB, both health and acl handlers use the same logic.
	getObj := func(change memdb.Change) interface{} {
		if change.Deleted() {
			return change.Before
		}
		return change.After
	}

	// TODO: mapping of table->topic?
	for _, change := range changes {
		switch change.Table {
		case "acl-tokens":
			token := getObj(change).(*structs.ACLToken)
			e := stream.Event{
				Topic:   stream.Topic_ACLTokens,
				Index:   tx.Index,
				Payload: token,
			}
			events = append(events, e)
		case "acl-policies":
			policy := getObj(change).(*structs.ACLPolicy)
			e := stream.Event{
				Topic:   stream.Topic_ACLPolicies,
				Index:   tx.Index,
				Payload: policy,
			}
			events = append(events, e)
		case "acl-roles":
			role := getObj(change).(*structs.ACLRole)
			e := stream.Event{
				Topic:   stream.Topic_ACLRoles,
				Index:   tx.Index,
				Payload: role,
			}
			events = append(events, e)
		}
	}
	return events, nil
}
