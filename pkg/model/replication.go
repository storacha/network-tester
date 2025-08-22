package model

import (
	"time"

	"github.com/google/uuid"
)

type Replication struct {
	ID        uuid.UUID `json:"id"`
	Region    string    `json:"region"`    // region the test is performed from
	Shard     Link      `json:"shard"`     // the shard for which replication was requested
	Cause     Link      `json:"cause"`     // the space/blob/replicate invocation
	Replicas  int       `json:"replicas"`  // number of replicas requested in the invocation
	Transfers LinkList  `json:"transfers"` // blob/replica/transfer tasks
	Requested time.Time `json:"requested"` // when the space/blob/replicate invocation was sent
	Error     Error     `json:"error"`
}
