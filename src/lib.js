import fs from 'node:fs'
import wordListPath from 'word-list'

/** @typedef {() => number} RandomNumberGenerator */

/**
 * @param {number} min
 * @param {number} max
 * @param {object} [options]
 * @param {RandomNumberGenerator} [options.rng]
 */
export const randomInt = (min, max, options) => {
  const rng = options?.rng ?? Math.random
  return Math.floor(rng() * (max - min) + min)
}

/**
 * Get an integer, skewed towards the minimum.
 * https://stackoverflow.com/questions/8435183/generate-a-weighted-random-number#answer-78321754
 *
 * @param {number} strength
 * @param {number} min
 * @param {number} max
 * @param {object} [options]
 * @param {RandomNumberGenerator} [options.rng]
 */
export const randomSkewedInt = (strength, min, max, options) => {
  const rng = options?.rng ?? Math.random
  let randPow = Math.pow(rng(), 1 / strength)
  randPow = 1 - randPow
  return Math.floor(randPow * (max - min + 1)) + min
}

/**
 * @template T
 * @param {T[]} arr
 * @param {object} [options]
 * @param {RandomNumberGenerator} [options.rng]
 * @returns {T}
 */
export const randomChoice = (arr, options) => arr[randomInt(0, arr.length, options)]

const words = (await fs.promises.readFile(wordListPath, 'utf8')).split('\n')
const exts = ['gif', 'jpg', 'png', 'json', 'svg', 'html', 'csv', 'mp3', 'txt', 'pdf', 'docx', 'xml', 'woff']

/**
 * @param {object} [options]
 * @param {RandomNumberGenerator} [options.rng]
 */
export const randomFileName = (options) => `${randomChoice(words, options)}.${randomChoice(exts, options)}`

export const kb = 1024
export const mb = 1024 * kb
export const gb = 1024 * mb
