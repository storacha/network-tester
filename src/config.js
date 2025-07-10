import fs from 'node:fs'
import path from 'node:path'
import dotenv from 'dotenv'
import * as Service from '@storacha/client/service'
import * as Proof from '@storacha/client/proof'
import * as Ed25519 from '@storacha/client/principal/ed25519'
import * as DID from '@ipld/dag-ucan/did'
import Package from '../package.json' with { type: 'json' }
import { gb, mb } from './lib.js'

dotenv.config()

/** Geographic region where the test is being run. */
export const region = process.env.REGION || 'unknown'

/** Maximum number of bytes to generate/store across all uploads. */
export const maxBytes = 100 * gb

/** Maximum bytes for a single upload. */
// const maxPerUploadBytes = 4 * gb
export const maxPerUploadBytes = 5 * mb

/** Maximum CAR shard size. */
export const maxShardSize = 512 * mb

const headers = { ...Service.defaultHeaders }
headers['X-Client'] += ' UploadTester/' + Package.version.split('.')[0]

const stagingWarmConnection = Service.uploadServiceConnection({
  id: DID.parse('did:web:staging.up.warm.storacha.network'),
  url: new URL('https://staging.up.warm.storacha.network'),
  headers
})

const hotConnection = Service.uploadServiceConnection({
  id: DID.parse('did:web:up.storacha.network'),
  url: new URL('https://up.storacha.network'),
  headers
})

export const connection = process.env.NETWORK === 'staging-warm'
  ? stagingWarmConnection
  : hotConnection

export const id = Ed25519.parse(process.env.PRIVATE_KEY ?? '')

export const proof = await Proof.parse(process.env.PROOF ?? '')

export const spaceDID = DID.parse(proof.capabilities[0].with).did()

export const dataDir = path.join(import.meta.dirname, '..', 'data')

await fs.promises.mkdir(dataDir, { recursive: true })
