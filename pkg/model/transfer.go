package model

import (
	"time"

	"github.com/google/uuid"
)

type ReplicaTransfer struct {
	ID                 Link      `json:"id"`                 // CID of the blob/replica/transfer task
	Replication        uuid.UUID `json:"replication"`        // parent replication
	Node               DID       `json:"node"`               // node that replicated the data
	LocationCommitment Link      `json:"locationCommitment"` // issued location commitment for replicated data
	URL                URL       `json:"url"`                // URL from the location commitment
	Started            time.Time `json:"started"`            // time we started tracking the transfer (soon after the initial replication request)
	Ended              time.Time `json:"ended"`              // time we received a receipt for the transfer (minus up to the polling interval)
	Error              Error     `json:"error"`
}
