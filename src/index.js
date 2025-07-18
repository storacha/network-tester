import path from 'node:path'
import { inspect } from 'node:util'
import formatBytes from 'bytes'
import formatDuration from 'humanize-duration'
import * as Link from 'multiformats/link'
import { sha256 } from 'multiformats/hashes/sha2'
import { base58btc } from 'multiformats/bases/base58'
import { v4 as generateUUID } from 'uuid'
import { BlockStream, code as carCode } from '@storacha/upload-client/car'
import { ShardingStream } from '@storacha/upload-client/sharding'
import * as Blob from '@storacha/upload-client/blob'
import * as Index from '@storacha/upload-client/index'
import * as Upload from '@storacha/upload-client/upload'
import { indexShardedDAG } from '@storacha/blob-index'
import seedRandom from 'seedrandom'
import { id, proof, spaceDID, region, maxBytes, maxPerUploadBytes, maxShardSize, connection, dataDir, replicas } from './config.js'
import { generateSource, minFileSize } from './gen.js'
import * as EventLog from './event-log.js'

/**
 * @import * as API from '@storacha/client/types'
 * @import * as UploadAPI from '@storacha/upload-client/types'
 * @import * as IndexerAPI from '@storacha/indexing-service-client/api'
 */

const invocationConf = {
  issuer: id,
  audience: connection.id,
  with: spaceDID,
  proofs: [proof]
}
const options = { connection }

const [sourceLog, shardLog, replicationLog, uploadLog] = await Promise.all([
  EventLog.create(path.join(dataDir, 'sources.csv')),
  EventLog.create(path.join(dataDir, 'shards.csv')),
  EventLog.create(path.join(dataDir, 'replications.csv')),
  EventLog.create(path.join(dataDir, 'uploads.csv'))
])

console.log('Region:')
console.log(`  ${process.env.REGION ?? 'unknown'}`)
console.log('Network:')
console.log(`  ${process.env.NETWORK ?? 'hot'}`)
console.log('Agent:')
console.log(`  ${id.did()}`)
console.log('Space:')
console.log(`  ${spaceDID}`)

let totalSize = 0
let totalFiles = 0
let totalSources = 0
let totalSourceFiles = 0
let totalSourceDirectories = 0
let totalSourceShardedDirectories = 0

while (totalSize < maxBytes) {
  const maxSize = Math.min(maxPerUploadBytes, maxBytes - totalSize)
  if (maxSize < minFileSize) break

  const start = new Date()
  const sourceID = generateUUID()
  const rng = seedRandom(sourceID)
  const source = generateSource({ maxSize, rng })

  console.log('Source:')
  console.log(`  ${sourceID}`)
  console.log('    type:', source.type)
  console.log('    count:', source.count)
  console.log('    size:', formatBytes(source.size))

  await sourceLog.append({
    id: sourceID,
    region,
    type: source.type,
    count: source.count,
    size: source.size,
    created: start.toISOString()
  })

  const uploadID = generateUUID()
  /** @type {Array<Map<UploadAPI.SliceDigest, UploadAPI.Position>>} */
  const shardIndexes = []
  /** @type {UploadAPI.CARLink[]} */
  const shards = []
  /** @type {UploadAPI.AnyLink?} */
  let root = null
  let uploadSuccess = false
  let error = ''

  try {
    await new BlockStream(source)
      .pipeThrough(new ShardingStream({ shardSize: maxShardSize }))
      .pipeThrough(
        /** @type {TransformStream<UploadAPI.IndexedCARFile, UploadAPI.CARMetadata>} */
        (new TransformStream({
          async transform(car, controller) {
            const bytes = new Uint8Array(await car.arrayBuffer())
            const digest = await sha256.digest(bytes)
            const cid = Link.create(carCode, digest)
            const start = new Date()
            let error = ''
            /** @type {API.Delegation<[IndexerAPI.AssertLocation]>|undefined} */
            let site
            try {
              const res = await Blob.add(invocationConf, digest, bytes, options)
              site = res.site
              const { version, roots, size, slices } = car
              controller.enqueue({ version, roots, size, cid, slices })
              totalSize += size
            } catch (err) {
              error = inspect(err, { depth: 50 })
              throw err
            } finally {
              const url = site ? site.capabilities[0].nb.location[0] : ''
              const end = new Date()

              console.log('Shard:')
              console.log(`  ${cid}`)
              console.log(`    size: ${formatBytes(car.size)}`)
              console.log(`    url: ${url}`)
              if (error) console.log(`    error: ${error}`)
              console.log(`    elapsed: ${formatDuration(end.getTime() - start.getTime())}`)

              await shardLog.append({
                id: cid.toString(),
                source: sourceID,
                upload: uploadID,
                node: site ? site.issuer.did() : '',
                locationCommitment: site ? site.cid.toString() : '',
                url,
                size: car.size,
                error,
                started: start.toISOString(),
                ended: end.toISOString()
              })
            }

            // If we sucessfully received a location commitment, and the number
            // of wanted replicas is more than 1, then ask for replicas to be
            // made.
            if (site && replicas > 1) {
              let error = ''
              /** @type {API.UnknownLink[]} */
              let tasks = []
              try {
                const res = await Blob.replicate(
                  invocationConf,
                  { digest, size: car.size },
                  site,
                  replicas,
                  options
                )
                // Note: we are not waiting for these tasks to complete
                tasks = res.site.map(s => s['ucan/await'][1])
              } catch (err) {
                error = inspect(err, { depth: 50 })
                throw err
              } finally {
                console.log('Replication:')
                console.log(`  ${cid}`)
                if (tasks.length) {
                  console.log('    tasks:')
                  for (const t of tasks) {
                    console.log(`      ${t}`)
                  }
                }
                if (error) console.log(`    error: ${error}`)

                await replicationLog.append({
                  id: cid.toString(),
                  source: sourceID,
                  upload: uploadID,
                  tasks: tasks.map(t => t.toString()).join('\n'),
                  error,
                  created: new Date().toISOString()
                })
              }
            }
          }
        })))
      .pipeTo(
        new WritableStream({
          write(meta) {
            root = root || meta.roots[0]
            shards.push(meta.cid)
            // add the CAR shard itself to the slices
            meta.slices.set(meta.cid.multihash, [0, meta.size])
            shardIndexes.push(meta.slices)
          }
        })
      )
    uploadSuccess = true
  } catch (err) {
    error = inspect(err, { depth: 50 })
  }

  let indexSuccess = false
  /** @type {UploadAPI.CARLink|undefined} */
  let indexLink
  try {
    if (uploadSuccess) {
      if (!root) throw new Error('missing root CID')

      const indexBytes = await indexShardedDAG(root, shards, shardIndexes)
      if (!indexBytes.ok) {
        throw new Error('failed to archive DAG index', { cause: indexBytes.error })
      }
      const indexDigest = await sha256.digest(indexBytes.ok)
      indexLink = Link.create(carCode, indexDigest)

      try {
        await Blob.add(invocationConf, indexDigest, indexBytes.ok, options)
        totalSize += indexBytes.ok.length
      } catch (err) {
        throw new Error(`adding index blob: ${base58btc.encode(indexDigest.bytes)}`, { cause: err })
      }
      try {
        await Index.add(invocationConf, indexLink, options)
      } catch (err) {
        throw new Error(`adding index: ${indexLink}`, { cause: err })
      }
      try {
        await Upload.add(invocationConf, root, shards, options)
      } catch (err) {
        throw new Error(`adding upload: ${root}`, { cause: err })
      }

      indexSuccess = true
    }
  } catch (err) {
    error = inspect(err, { depth: 50 })
  } finally {
    const end = new Date()
    await uploadLog.append({
      id: uploadID,
      // @ts-expect-error
      root: root ? root.toString() : '',
      source: sourceID,
      upload: uploadID,
      index: indexLink ? indexLink.toString() : '',
      shards: shards.map(s => s.toString()).join('\n'),
      error,
      started: start.toISOString(),
      ended: end.toISOString()
    })
    
    console.log('Upload:')
    console.log(`  ${uploadID}`)
    console.log(`    root: ${root ?? ''}`)
    console.log(`    index: ${indexLink ?? ''}`)
    console.log(`    shards:`)
    for (const s of shards) {
      console.log(`      ${s}`)
    }
    if (error) console.log(`  error: ${error}`)
    console.log(`  elapsed: ${formatDuration(end.getTime() - start.getTime())}`)
  }

  if (uploadSuccess && indexSuccess) {
    totalSources++
    totalFiles += source.count
    if (source.type === 'file') {
      totalSourceFiles++
    } else if (source.type === 'directory') {
      totalSourceDirectories++
    } else if (source.type === 'sharded-directory') {
      totalSourceShardedDirectories++
    }
  }

  console.log('Summary:')
  console.log(`  sources: ${totalSources.toLocaleString()}`)
  console.log(`    file: ${totalSourceFiles.toLocaleString()}`)
  console.log(`    directory: ${totalSourceDirectories.toLocaleString()}`)
  console.log(`    sharded-directory: ${totalSourceShardedDirectories.toLocaleString()}`)
  console.log(`  files: ${totalFiles.toLocaleString()}`)
  console.log(`  size: ${formatBytes(totalSize)}`)
}
