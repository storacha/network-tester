import path from 'node:path'
import formatBytes from 'bytes'
import * as Link from 'multiformats/link'
import { sha256 } from 'multiformats/hashes/sha2'
import { BlockStream, code as carCode } from '@storacha/upload-client/car'
import { ShardingStream } from '@storacha/upload-client/sharding'
import * as Blob from '@storacha/upload-client/blob'
import * as Index from '@storacha/upload-client/index'
import * as Upload from '@storacha/upload-client/upload'
import { indexShardedDAG } from '@storacha/blob-index'
import { id, proof, spaceDID, region, maxBytes, maxPerUploadBytes, maxShardSize, connection, dataDir } from './config.js'
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

const [sourceLog, shardLog, uploadLog] = await Promise.all([
  EventLog.create(path.join(dataDir, 'sources.csv')),
  EventLog.create(path.join(dataDir, 'shards.csv')),
  EventLog.create(path.join(dataDir, 'uploads.csv'))
])

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

  const source = generateSource({ maxSize })

  console.log('Source:')
  console.log(`  ${source.id}`)
  console.log('    type:', source.type)
  console.log('    count:', source.count)
  console.log('    size:', formatBytes(source.size))

  await sourceLog.append({
    id: source.id,
    region,
    type: source.type,
    count: source.count,
    size: source.size,
    created: new Date().toISOString()
  })

  totalSize += source.size

  /** @type {Array<Map<UploadAPI.SliceDigest, UploadAPI.Position>>} */
  const shardIndexes = []
  /** @type {UploadAPI.CARLink[]} */
  const shards = []
  /** @type {UploadAPI.AnyLink?} */
  let root = null
  let uploadSuccess = false

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
            const created = new Date()
            let error = ''
            /** @type {API.Delegation<[IndexerAPI.AssertLocation]>|undefined} */
            let site
            try {
              // Invoke blob/add and write bytes to write target
              const res = await Blob.add(invocationConf, digest, bytes, options)
              site = res.site

              const { version, roots, size, slices } = car

              controller.enqueue({ version, roots, size, cid, slices })
            } catch (/** @type {any} */ err) {
              error = err.stack ?? err.message ?? String(err)
              throw err
            } finally {
              const url = site ? site.capabilities[0].nb.location[0] : ''

              console.log('Shard:')
              console.log(`  ${cid}`)
              console.log(`    size: ${formatBytes(car.size)}`)
              console.log(`    url: ${url}`)
              if (error) console.log(`    error: ${error}`)

              await shardLog.append({
                id: cid.toString(),
                source: source.id,
                locationCommitment: site ? site.cid.toString() : '',
                url,
                size: car.size,
                error,
                created: created.toISOString(),
                transferred: new Date().toISOString()
              })
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
    console.error(`Error: uploading source: ${source.id}`, err)
  }

  if (!root) throw new Error('missing root CID')

  let indexSuccess = false
  if (uploadSuccess) {
    const indexBytes = await indexShardedDAG(root, shards, shardIndexes)

    if (!indexBytes.ok) {
        throw new Error('failed to archive DAG index', { cause: indexBytes.error })
    }
    const indexDigest = await sha256.digest(indexBytes.ok)
    const indexLink = Link.create(carCode, indexDigest)

    try {
      await Blob.add(invocationConf, indexDigest, indexBytes.ok, options)
      await Index.add(invocationConf, indexLink, options)
      await Upload.add(invocationConf, root, shards, options)

      await uploadLog.append({
        // @ts-expect-error
        id: root.toString(),
        source: source.id,
        index: indexLink.toString(),
        shards: shards.map(s => s.toString()).join('\n'),
        created: new Date().toISOString()
      })
      
      console.log('Upload:')
      console.log(`  ${root}`)
      console.log(`    shards:`)
      for (const s of shards) {
        console.log(`      ${s}`)
      }

      indexSuccess = true
    } catch (err) {
      console.error(`Error: uploading index for source: ${source.id}`, err)
    }
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
