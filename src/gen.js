import { Readable } from 'node:stream'
import { CARWriterStream } from 'carstream'
import { createDirectoryEncoderStream, createFileEncoderStream } from '@storacha/upload-client/unixfs'
import randomBytesReadableStream from 'random-bytes-readable-stream'
import { v4 as generateUUID } from 'uuid'
import { randomChoice, randomFileName, randomSkewedInt, gb, mb, randomInt } from './lib.js'

/** @import { BlobLike, FileLike, UnixFSEncoderSettingsOptions } from '@storacha/upload-client/types' */

/**
 * @typedef {'file'|'directory'|'sharded-directory'} SourceType
 * @typedef {{
 *   type?: SourceType
 *   maxSize?: number
 *   unixfsSettings?: UnixFSEncoderSettingsOptions['settings']
 * }} Options
 */

const FileSource = /** @type {SourceType} */ ('file')
const DirectorySource = /** @type {SourceType} */ ('directory')
const ShardedDirectorySource = /** @type {SourceType} */ ('sharded-directory')

/**
 * By default, 55% of the time we'll generate a directory, 40% of the time we'll
 * generate a file and 5% of the time we'll generate a HAMT sharded directory.
 */
const weightedTypes = [
  ...Array(40).fill(FileSource),
  ...Array(55).fill(DirectorySource),
  ...Array(5).fill(ShardedDirectorySource)
]

/** The default minimum file size. */
export const minFileSize = 128

/** The default maximum size of all generated files. */
export const maxSizeTotal = 4 * gb

/** @param {Options} [options] */
export const generateSource = (options) => {
  let type = options?.type
  if (!type) {
    const maxSize = options?.maxSize ?? maxSizeTotal
    if (maxSize < 1000 * minFileSize) {
      // if less than the minimum sharded directory size, choose a file
      type = 'file'
    } else {
      type = randomChoice(weightedTypes)
    }
  }

  switch (type) {
    case FileSource:
      return generateFile(options)
    case DirectorySource:
      return generateDirectory(options)
    case ShardedDirectorySource:
      return generateShardedDirectory(options)
    default:
      throw new Error(`unknown type: ${type}`)
  }
}

/** @param {Options} [options] */
const generateFile = (options) => {
  const maxSize = options?.maxSize ?? maxSizeTotal
  if (minFileSize > maxSize) {
    throw new Error(`min file size (${minFileSize} bytes) is greater than max (${maxSize} bytes)`)
  }
  const totalSize = maxSize < (50 * mb)
    ? randomInt(minFileSize, maxSize)
    : randomSkewedInt(3, minFileSize, maxSize)

  return {
    id: generateUUID(),
    type: FileSource,
    size: totalSize,
    count: 1,
    stream: () => {
      const blob =
        /** @type {BlobLike} */
        ({ stream: () => Readable.toWeb(randomBytesReadableStream({ size: totalSize })) })
      return createFileEncoderStream(blob, { settings: options?.unixfsSettings })
        .pipeThrough(new CARWriterStream())
    }
  }
}

/** @param {Options} [options] */
const generateDirectory = (options) => {
  const maxSize = options?.maxSize ?? maxSizeTotal
  if (minFileSize > maxSize) {
    throw new Error(`min file size (${minFileSize} bytes) is greater than max (${maxSize} bytes)`)
  }
  const totalSize = maxSize < (50 * mb)
    ? randomInt(minFileSize, maxSize)
    : randomSkewedInt(3, minFileSize, maxSize)

  let currentSize = 0
  const files = /** @type {FileLike[]} */ ([])
  for (let i = 0; i < 1000; i++) {
    const name = randomFileName()
    const size = i === 999
      ? totalSize - currentSize
      : randomSkewedInt(3, minFileSize, totalSize - currentSize)
    const stream = () => Readable.toWeb(randomBytesReadableStream({ size }))
    files.push(/** @type {FileLike} */ ({ name, size, stream }))
    currentSize += size
    if (currentSize >= totalSize) {
      break
    }
  }

  return {
    id: generateUUID(),
    type: DirectorySource,
    size: totalSize,
    count: files.length,
    stream: () =>
      createDirectoryEncoderStream(files, { settings: options?.unixfsSettings })
        .pipeThrough(new CARWriterStream())
  }
}

/** @param {Options} [options] */
const generateShardedDirectory = (options) => {
  const maxSize = options?.maxSize ?? maxSizeTotal
  if (minFileSize > maxSize) {
    throw new Error(`min file size (${minFileSize} bytes) is greater than max (${maxSize} bytes)`)
  }
  if (1000 * minFileSize > maxSize) {
    throw new Error(`unable to fit >1,000 files of ${minFileSize} bytes into max (${maxSize} bytes)`)
  }
  const totalSize = maxSize < (50 * mb)
    ? randomInt(1000 * minFileSize, maxSize)
    : randomSkewedInt(3, 1000 * minFileSize, maxSize)

  let currentSize = 0
  const files = /** @type {FileLike[]} */ ([])
  for (let i = 0; i < 1000; i++) {
    const name = randomFileName()
    const size = minFileSize
    const stream = () => Readable.toWeb(randomBytesReadableStream({ size }))
    files.push(/** @type {FileLike} */ ({ name, size, stream }))
    currentSize += size
  }

  while (currentSize < totalSize) {
    const name = randomFileName()
    const size = randomSkewedInt(3, minFileSize, totalSize - currentSize)
    const stream = () => Readable.toWeb(randomBytesReadableStream({ size }))
    files.push(/** @type {FileLike} */ ({ name, size, stream }))
    currentSize += size
  }

  return {
    id: generateUUID(),
    type: ShardedDirectorySource,
    size: totalSize,
    count: files.length,
    stream: () =>
      createDirectoryEncoderStream(files, { settings: options?.unixfsSettings })
        .pipeThrough(new CARWriterStream())
  }
}
