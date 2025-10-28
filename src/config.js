import fs from 'node:fs'
import path from 'node:path'
import dotenv from 'dotenv'
import * as Service from '@storacha/client/service'
import * as Proof from '@storacha/client/proof'
import * as Ed25519 from '@storacha/client/principal/ed25519'
import { SHARD_SIZE } from '@storacha/upload-client/sharding'
import * as DID from '@ipld/dag-ucan/did'
import Package from '../package.json' with { type: 'json' }
import { gb } from './lib.js'

dotenv.config()

export const network = process.env.NETWORK

/** Geographic region where the test is being run. */
export const region = process.env.REGION || 'unknown'

/** Maximum number of bytes to generate/store across all uploads. */
export const maxBytes = process.env.MAX_BYTES
  ? parseInt(process.env.MAX_BYTES)
  : 100 * gb

/** Maximum bytes for a single upload. */
export const maxPerUploadBytes = process.env.MAX_PER_UPLOAD_BYTES
  ? parseInt(process.env.MAX_PER_UPLOAD_BYTES)
  : 1 * gb

/** Maximum CAR shard size. */
export const maxShardSize = network === 'staging-warm'
  ? 266_338_304 // https://gist.github.com/alanshaw/be76c3d4ff555c3a0ee9f5b6e96b5436
  : SHARD_SIZE

const headers = { ...Service.defaultHeaders }
headers['X-Client'] += ' UploadTester/' + Package.version.split('.')[0]

const stagingWarmUploadConnection = Service.uploadServiceConnection({
  id: DID.parse('did:web:staging.up.warm.storacha.network'),
  url: new URL('https://staging.up.warm.storacha.network'),
  headers
})

const stagingUploadConnection = Service.uploadServiceConnection({
  id: DID.parse('did:web:staging.up.storacha.network'),
  url: new URL('https://staging.up.storacha.network'),
  headers
})

const hotUploadConnection = Service.uploadServiceConnection({
  id: DID.parse('did:web:up.storacha.network'),
  url: new URL('https://up.storacha.network'),
  headers
})

export const uploadConnection = network === 'staging-warm'
  ? stagingWarmUploadConnection
  : network === 'staging'
    ? stagingUploadConnection
    : hotUploadConnection

export const id = Ed25519.parse(process.env.PRIVATE_KEY ?? '')

export const proof = await Proof.parse(process.env.PROOF ?? '')

export const spaceDID =
  /** @type {import('@storacha/client/types').SpaceDID} */
  (DID.parse(proof.capabilities[0].with).did())

export const dataDir = path.join(import.meta.dirname, '..', 'data')

await fs.promises.mkdir(dataDir, { recursive: true })
