# Network Tester

Test uploads and more to the Storacha Network.

## Getting started

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
* `data/replications.csv` - information about replication tasks requested for a given shard.
* `data/uploads.csv` - information about each upload that is performed, i.e. the shards and the DAG root CID.

## About

The upload tests aim to upload a configured number of bytes to the service (default 100 GiB). The data is sent as many randomly sized uploads in order to simulate typical upload patterns. Additionally within a given upload, data is sharded - per the usual upload flow. All these values are configurable.

For a given upload the script will generate either a single file, directory of files or a sharded diretory of files of varying sizes. The maximum size of a single upload can also be configured , but is set by default to 4 GiB.

All configuration values can be found in [config.js](./src/config.js).

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
    * Size
    * Target node DID
    * Location commitment CID
    * Location commitment URL
    * Error details
    * Created at
    * Transferred at
* Replication
    * ID (shard CID)
    * Source ID
    * Replication task CIDs
    * Error details
    * Created at
* Upload
    * ID (DAG root CID)
    * Source ID
    * Shard CIDs
    * Created at
