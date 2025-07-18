package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-libstoracha/capabilities/assert"
	"github.com/storacha/go-ucanto/core/dag/blockstore"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/indexing-service/pkg/blobindex"
	"github.com/storacha/indexing-service/pkg/client"
	"github.com/storacha/indexing-service/pkg/types"
	"github.com/stretchr/testify/require"
)

type Link struct {
	cidlink.Link
}

func (l Link) MarshalJSON() ([]byte, error) {
	if l == (Link{}) {
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
	message string
}

func (e Error) Error() string {
	return e.message
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
	Upload  uuid.UUID `json:"upload"`
	Index   Link      `json:"index"`
	Shards  LinkList  `json:"shards"`
	Error   Error     `json:"error"`
	Started time.Time `json:"started"`
	Ended   time.Time `json:"ended"`
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
		if u.Error.Error() != "" {
			fmt.Printf("Skipping failed upload: %s\n", u.ID)
			continue
		}
		fmt.Println("Upload")
		fmt.Printf("  %s\n", u.ID)
		fmt.Printf("    root: %s\n", u.Root.String())
		fmt.Printf("    source: %s\n", u.Source)
		fmt.Printf("    index: %s\n", u.Index.String())
		fmt.Printf("    shards: %v\n", u.Shards)
		fmt.Printf("    started: %s\n", u.Started.Format(time.DateTime))
		fmt.Printf("    ended: %s\n", u.Ended.Format(time.DateTime))

		c, err := client.New(IndexingServicePrincipal, IndexingServiceURL)
		require.NoError(t, err)

		result, err := c.QueryClaims(t.Context(), types.Query{
			Hashes: []multihash.Multihash{u.Root.Hash()},
		})
		require.NoError(t, err)

		if len(result.Indexes()) == 0 {
			require.Fail(t, fmt.Sprintf("no results for root CID: %s", u.Root))
		}
		if !slices.ContainsFunc(result.Indexes(), func(l ipld.Link) bool {
			return l.String() == u.Index.String()
		}) {
			require.Fail(t, fmt.Sprintf("index not found in query results: %s", u.Index))
		}

		indexURL, err := extractLocationURL(u.Index.Hash(), result)
		require.NoError(t, err)

		res, err := http.Get(indexURL.String())
		require.NoError(t, err)

		// TODO: verify body hashes to index CID

		index, err := blobindex.Extract(res.Body)
		require.NoError(t, err)

		fmt.Println("Index")
		fmt.Printf("  %s\n", u.Index)
		fmt.Printf("    url: %s\n", indexURL.String())

		for shardDigest, slices := range index.Shards().Iterator() {
			shardURL, err := extractLocationURL(shardDigest, result)
			require.NoError(t, err)

			fmt.Println("Shard")
			fmt.Printf("  z%s\n", shardDigest.B58String())
			fmt.Printf("    url: %s\n", shardURL.String())
			fmt.Println("    slices:")

			for sliceDigest, position := range slices.Iterator() {
				req, err := http.NewRequest(http.MethodGet, shardURL.String(), nil)
				require.NoError(t, err)
				req.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", position.Offset, position.Offset+position.Length-1))
				res, err := http.DefaultClient.Do(req)
				require.NoError(t, err)
				data, err := io.ReadAll(res.Body)
				require.NoError(t, err)
				digest, err := multihash.Sum(data, multihash.SHA2_256, -1)
				require.NoError(t, err)
				require.Equal(t, digest, sliceDigest)
				fmt.Printf("      z%s @ %d-%d\n", sliceDigest.B58String(), position.Offset, position.Offset+position.Length-1)
			}
		}

		fmt.Printf("%s passed\n", u.ID)
	}
}

type source struct {
	capability ucan.Capability[any]
	delegation delegation.Delegation
}

func (s source) Capability() ucan.Capability[any] {
	return s.capability
}

func (s source) Delegation() delegation.Delegation {
	return s.delegation
}

// extractLocationURL inspects indexing service query results to find a location
// commitment for the passed digest. The URL is extracted and returned.
func extractLocationURL(digest multihash.Multihash, result types.QueryResult) (url.URL, error) {
	bs, err := blockstore.NewBlockReader(blockstore.WithBlocksIterator(result.Blocks()))
	if err != nil {
		return url.URL{}, err
	}

	for _, link := range result.Claims() {
		d, err := delegation.NewDelegationView(link, bs)
		if err != nil {
			return url.URL{}, err
		}

		match, err := assert.Location.Match(source{
			capability: d.Capabilities()[0],
			delegation: d,
		})
		if err != nil {
			continue
		}

		cap := match.Value()
		if bytes.Equal(cap.Nb().Content.Hash(), digest) {
			return cap.Nb().Location[0], nil
		}
	}

	return url.URL{}, fmt.Errorf("no location commitment in results for: z%s", digest.B58String())
}
