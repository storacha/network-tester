package model

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

type Link struct {
	cidlink.Link
}

func (l Link) MarshalJSON() ([]byte, error) {
	emptyLink := Link{}
	if l == emptyLink {
		return json.Marshal("")
	}
	return json.Marshal(l.String())
}

func (l *Link) UnmarshalJSON(b []byte) error {
	var str string
	err := json.Unmarshal(b, &str)
	if err != nil {
		return fmt.Errorf("parsing string: %w", err)
	}
	if str == "" {
		return nil
	}
	cid, err := cid.Decode(str)
	if err != nil {
		return fmt.Errorf("parsing CID: %w", err)
	}
	*l = Link{cidlink.Link{Cid: cid}}
	return nil
}

type LinkList []Link

func (ll LinkList) MarshalJSON() ([]byte, error) {
	strLinks := make([]string, 0, len(ll))
	for _, l := range ll {
		strLinks = append(strLinks, l.String())
	}
	return json.Marshal(strings.Join(strLinks, "\n"))
}

func (ll *LinkList) UnmarshalJSON(b []byte) error {
	var str string
	err := json.Unmarshal(b, &str)
	if err != nil {
		return fmt.Errorf("parsing string: %w", err)
	}
	if str == "" {
		return nil
	}
	var links LinkList
	for str := range strings.SplitSeq(str, "\n") {
		cid, err := cid.Decode(str)
		if err != nil {
			return fmt.Errorf("decoding link list CID: %w", err)
		}
		link := Link{cidlink.Link{Cid: cid}}
		links = append(links, link)
	}
	*ll = links
	return nil
}

type Error struct {
	Message string
}

func (e Error) Error() string {
	return e.Message
}

func (e Error) MarshalJSON() ([]byte, error) {
	return json.Marshal(e.Error())
}

func (e *Error) UnmarshalJSON(b []byte) error {
	var str string
	err := json.Unmarshal(b, &str)
	if err != nil {
		return nil
	}
	*e = Error{str}
	return nil
}

type Upload struct {
	ID      uuid.UUID `json:"id"`
	Root    Link      `json:"root"`
	Source  uuid.UUID `json:"source"`
	Index   Link      `json:"index"`
	Shards  LinkList  `json:"shards"`
	Error   Error     `json:"error"`
	Started time.Time `json:"started"`
	Ended   time.Time `json:"ended"`
}
