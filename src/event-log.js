import fs from 'node:fs'
import path from 'node:path'

/**
 * @template {Record<string, string|number>} T
 * @typedef {{ append: (obj: T) => Promise<void> }} EventLog
 */

/**
 * @template {Record<string, string|number>} T
 * @param {string} filePath
 * @returns {Promise<EventLog<T>>}
 */
export const create = async (filePath) => {
  await fs.promises.mkdir(path.dirname(filePath), { recursive: true })
  const file = await fs.promises.open(filePath, 'a')
  return new CSVEventLog(file)
}

/**
 * @template {Record<string, string|number>} T
 * @implements {EventLog<T>}
 */
class CSVEventLog {
  #first
  #file

  /** @param {fs.promises.FileHandle} fileHandle */
  constructor (fileHandle) {
    this.#first = true
    this.#file = fileHandle
  }

  /** @type {EventLog<T>['append']} */
  async append (obj) {
    const keys = Object.keys(obj).sort()
    if (this.#first) {
      const line = keys.join(',') + '\n'
      await this.#file.write(new TextEncoder().encode(line))
      this.#first = false
    }
    const line = keys
      .map(k => {
        const value = obj[k]
        if (typeof value === 'string' && (value.includes(',') || value.includes('"') || value.includes('\n'))) {
          return `"${value.replaceAll('"', '""')}"`
        }
        return value
      })
      .join(',') + '\n'
    await this.#file.write(new TextEncoder().encode(line))
  }
}
