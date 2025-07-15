package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/stretchr/testify/require"
)

type ID struct {
	uuid.UUID
}

func (id ID) MarshalJSON() ([]byte, error) {
	return json.Marshal(id.String())
}

func (id *ID) UnmarshalJSON(b []byte) error {
	uuid, err := uuid.Parse(string(b))
	if err != nil {
		return fmt.Errorf("parsing UUID: %w", err)
	}
	id.UUID = uuid
	return nil
}

type Link struct {
	cidlink.Link
}

func (l Link) MarshalJSON() ([]byte, error) {
	if l.Cid == cid.Undef {
		return json.Marshal("")
	}
	return json.Marshal(l.String())
}

func (l *Link) UnmarshalJSON(b []byte) error {
	str := strings.Trim(string(b), "\"")
	if str == "" {
		l.Link = cidlink.Link{Cid: cid.Undef}
		return nil
	}
	cid, err := cid.Decode(str)
	if err != nil {
		return fmt.Errorf("parsing CID: %w", err)
	}
	l.Link = cidlink.Link{Cid: cid}
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

func (ll LinkList) UnmarshalJSON(b []byte) error {
	str := strings.Trim(string(b), "\"")
	if str == "" {
		return nil
	}
	strLinks := strings.SplitSeq(str, "\n")
	for str := range strLinks {
		cid, err := cid.Decode(str)
		if err != nil {
			return fmt.Errorf("decoding link list CID: %w", err)
		}
		link := Link{cidlink.Link{Cid: cid}}
		ll = append(ll, link)
	}
	return nil
}

type Error struct {
	error
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
	e.error = errors.New(str)
	return nil
}

type Timestamp struct {
	time.Time
}

func (ts Timestamp) MarshalJSON() ([]byte, error) {
	return json.Marshal(ts.Format(time.RFC3339))
}

func (ts *Timestamp) UnmarshalJSON(b []byte) error {
	time, err := time.Parse(time.RFC3339, strings.Trim(string(b), "\""))
	if err != nil {
		return fmt.Errorf("parsing timestamp: %w", err)
	}
	ts.Time = time
	return nil
}

type Upload struct {
	ID      ID        `json:"id"`
	Root    Link      `json:"root"`
	Source  ID        `json:"source"`
	Index   Link      `json:"index"`
	Shards  LinkList  `json:"shards"`
	Error   Error     `json:"error"`
	Started Timestamp `json:"started"`
	Ended   Timestamp `json:"ended"`
}

func IterateUploads(csvData io.Reader) iter.Seq2[Upload, error] {
	return func(yield func(Upload, error) bool) {
		uploads := csv.NewReader(csvData)
		var fields []string
		first := true
		for {
			record, err := uploads.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				yield(Upload{}, err)
				return
			}

			if first {
				fields = record
				first = false
				continue
			}

			data := map[string]string{}
			isRepeatedFields := true
			for i, k := range fields {
				if k != record[i] {
					isRepeatedFields = false
				}
				data[k] = record[i]
			}
			if isRepeatedFields {
				continue
			}
			jsonData, err := json.Marshal(data)
			if err != nil {
				yield(Upload{}, fmt.Errorf("marshalling JSON: %w", err))
				return
			}
			var upload Upload
			err = json.Unmarshal(jsonData, &upload)
			if err != nil {
				yield(Upload{}, fmt.Errorf("unmarshalling JSON: %w", err))
				return
			}
			if !yield(upload, nil) {
				return
			}
		}
	}
}

func TestRetrieval(t *testing.T) {
	uploadsData, err := os.Open("./data/uploads.csv")
	require.NoError(t, err)
	for u, err := range IterateUploads(uploadsData) {
		require.NoError(t, err)
		fmt.Printf("ID: %s\n", u.ID)
		fmt.Printf("Root: %s\n", u.Root.String())
		fmt.Printf("Source: %s\n", u.Source)
		fmt.Printf("Index: %s\n", u.Index.String())
		fmt.Printf("Shards: %v\n", u.Shards)
		fmt.Printf("Error: %s\n", u.Error.Error())
		fmt.Printf("Started: %s\n", u.Started.Format(time.DateTime))
		fmt.Printf("Ended: %s\n", u.Ended.Format(time.DateTime))
		fmt.Println("===")
		json, err := json.Marshal(u)
		require.NoError(t, err)
		fmt.Println(string(json))
	}
}
