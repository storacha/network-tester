import randomWord from 'random-word'

/**
 * @param {number} min
 * @param {number} max
 */
export const randomInt = (min, max) => Math.floor(Math.random() * (max - min) + min)

/**
 * Get an integer, skewed towards the minimum.
 * https://stackoverflow.com/questions/8435183/generate-a-weighted-random-number#answer-78321754
 *
 * @param {number} strength
 * @param {number} min
 * @param {number} max
 */
export const randomSkewedInt = (strength, min, max) => {
  let randPow = Math.pow(Math.random(), 1 / strength)
  randPow = 1 - randPow
  return Math.floor(randPow * (max - min + 1)) + min
}

/**
 * @template T
 * @param {T[]} arr
 * @returns {T}
 */
export const randomChoice = (arr) => arr[randomInt(0, arr.length)]

const exts = ['gif', 'jpg', 'png', 'json', 'svg', 'html', 'csv', 'mp3', 'txt', 'pdf', 'docx', 'xml', 'woff']

export const randomFileName = () => `${randomWord()}.${randomChoice(exts)}`

export const kb = 1024
export const mb = 1024 * kb
export const gb = 1024 * mb
