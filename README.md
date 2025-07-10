# Network Tester

Test uploads and more to the Storacha Network.

## Getting started

1. Install Node.js
2. Install the storacha CLI `npm install -g @storacha/cli`
3. Rename `.env.tpl` to `.env`
4. Create an identity for the upload tester `storacha key create`
5. Add private key to `.env`
6. Create a space for uploads to go to `storacha space create`
7. Delegate access to the space `storacha delegation create -c space/blob/add -c space/index/add -c upload/add did:key:uploadTester --base64`
8. Add delegation (proof) to `.env`
9. Set your region in `.env` to something sensible
10. Start the test using `npm start`

The script generates event logs to the following files:

* `data/source.csv` - information about the randomly generated source data. Each source has an ID that is referenced in other event logs.
* `data/shards.csv` - information about each shard that is stored to the service as part of an "upload".
* `data/uploads.csv` - information about each upload that is performed, i.e. the shards and the DAG root CID.
