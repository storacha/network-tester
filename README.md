# Network Tester

Test uploads and more to the Storacha Network.

## Getting started

### Upload Testing

1. Install Node.js
2. Clone the repo and cd into it
3. Install project dependencies `npm install`
4. Install the storacha CLI `npm install -g @storacha/cli`
5. Rename `.env.tpl` to `.env`
6. Create an identity for the upload tester `storacha key create`
7. Add private key to `.env`
8. Create a space for uploads to go to `storacha space create`
    * ⚠️ Ensure you set the correct [environment variables](https://gist.github.com/alanshaw/3c27e67bd9136c789e90950e3fc67644) if using non-production network.
9. Delegate access to the space `storacha delegation create -c space/blob/add -c space/blob/replicate -c space/index/add -c upload/add did:key:uploadTester --base64`
10. Add delegation (proof) to `.env`
11. Set your region in `.env` to something sensible
12. Start the test using `npm start`

The script generates event logs to the following files:

* `data/source.csv` - information about the randomly generated source data. Each source has an ID that is referenced in other event logs.
* `data/shards.csv` - information about each shard that is stored to the service as part of an "upload".
* `data/uploads.csv` - information about each upload that is performed, i.e. the shards and the DAG root CID.

### Retrieval Testing

1. Run the upload tests
2. Install Go
3. Start the test using `go run . ./data/uploads.csv >> ./data/retrievals.csv`

The script generates event logs to the following files:

* `data/retrieval.csv` - .

### Replication Testing

1. Run the upload tests
2. Install Go
3. Start the test using `go run . replication ./data/shards.csv ./data/replications.csv ./data/transfers.csv`

The script generates event logs to the following files:

* `data/replications.csv` - information about replication tasks requested for a given shard.
* `data/transfers.csv` - tracking of transfers for replicas.

## About

The upload tests aim to upload a configured number of bytes to the service (default 100 GiB). The data is sent as many randomly sized uploads in order to simulate typical upload patterns. Additionally within a given upload, data is sharded - per the usual upload flow. All these values are configurable.

For a given upload the script will generate either a single file, directory of files or a sharded diretory of files of varying sizes. The maximum size of a single upload can also be configured , but is set by default to 4 GiB.

All configuration values for upload tests can be found in [config.js](./src/config.js).

We collect information about the data sources created, the shards that are transferred, replications requested and the uploads that are registered:

* Source
    * ID (random UUID)
    * Region
    * Files count
    * Total files size
    * Created at
* Shard
    * ID (shard CID)
    * Source ID
    * Upload ID
    * Size
    * Target node DID
    * Location commitment CID
    * Location commitment URL
    * Error details
    * Transfer started at
    * Transfer ended at
* Upload
    * ID (random UUID)
    * DAG root CID
    * Source ID
    * Index CID
    * Shard CIDs
    * Error details
    * Upload started at
    * Upload ended at (including all shards, index and `upload/add` registration)

The retrieval tests use the output of the upload tests (specifically `uploads.csv`) to test data retrieval in a fasion similar to how the Storacha gateway operates.

Uploads are filtered so that only successful uploads are tested. The test script aims to extract every *block* of a DAG from storage nodes using HTTP byte range requests (HTTP GET requests using the `Range` header).

The script queries the indexing service using the DAG root as the query parameter.

The index is fetched directly from the node hosting the data using the location commitment returned in the results from the indexing service. Then the test will determine the location of each shard either by consulting the original query result or by making an additional query to the indexing service.

After the location of each shard is determined the test script uses the index to iterate over all the *slices* in each shard, making HTTP Range requests to extract the data. Content integrity is also verified.

The following data is collected for each retrieval request:

* Retrieval
    * ID (random UUID)
    * Region
    * Source ID
    * Upload ID
    * Node DID
    * Shard digest
    * Slice digest
    * Retrieval start time
    * Response start time (i.e. TTFB)
    * Retrieval end time
    * HTTP status code
    * Error details (if applicable)

The replication tests use the output of the upload tests (specifically `shards.csv`) to request replications for successfully uploaded shards. The script simply makes a `space/blob/replicate` invocation to the upload service and waits for the transfer tasks to complete (or fail/timeout) by polling the upload service receipts endpoint.

Since the replication tests are performed after the upload tests the location commitment needed for the `space/blob/replicate` invocation needs to be retrieved from the storage node that issued it. We indirectly do this by querying the indexing service.

* Replication
    * ID (random UUID)
    * Region
    * Shard ID (shard CID)
    * Cause invocation CID (`space/blob/replicate` invocation)
    * Requested replicas
    * Replication transfer task CIDs
    * Request time
    * Error details
* Replica Transfer
    * ID (transfer task CID)
    * Replication ID
    * DID of the node performing the replication
    * Location commitment CID
    * URL from the location commitment
    * Transfer start time (approx)
    * Transfer end time (approx)
    * Error details
