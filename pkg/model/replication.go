package model

import (
	"time"

	"github.com/google/uuid"
)

type Replication struct {
	ID        uuid.UUID `json:"id"`
	Shard     Link      `json:"shard"`     // The shard for which replication was requested
	Cause     Link      `json:"cause"`     // The space/blob/replicate invocation
	Replicas  int       `json:"replicas"`  // Number of replicas requested in the invocation
	Transfers LinkList  `json:"transfers"` // blob/replica/transfer tasks
	Requested time.Time `json:"requested"` // When the space/blob/replicate invocation was sent
	Error     Error     `json:"error"`
}
