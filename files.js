const jsesc = require('jsesc')
const sanitize = require('sanitize-filename')
const TypedFastBitSet = require('typedfastbitset')
const { readFile, writeFile } = require('atomically-universal')
const toBuffer = require('typedarray-to-buffer')

const FIELD_SIZE = 4 // bytes

/*
 * ## File format for tarr files
 *
 * Each header field is 4 bytes in size.
 *
 * | offset (bytes) | name    | type     |
 * |----------------|---------|----------|
 * | 0              | version | UInt32LE |
 * | 4              | offset  | UInt32LE |
 * | 8              | count   | UInt32LE |
 * | 12             | (N/A)   | (N/A)    |
 * | 16             | body    | Buffer   |
 *
 * Note that the 4th header field (offset=12) is empty on purpose, to support
 * `new Float64Array(b,start,len)` where `start` must be a multiple of 8
 * when loading in `loadTypedArrayFile`.
 */

function saveTypedArrayFile(filename, version, offset, count, tarr, cb) {
  if (!cb)
    cb = (err) => {
      if (err) console.error(err)
    }

  const dataBuffer = toBuffer(tarr)
  // we try to save an extra 10% so we don't have to immediately grow
  // after loading and adding again
  const saveSize = Math.min(count * 1.1, tarr.length)
  const b = Buffer.alloc(4 * FIELD_SIZE + saveSize * tarr.BYTES_PER_ELEMENT)
  b.writeUInt32LE(version, 0)
  b.writeUInt32LE(offset, FIELD_SIZE)
  b.writeUInt32LE(count, 2 * FIELD_SIZE)
  dataBuffer.copy(b, 4 * FIELD_SIZE)

  writeFile(filename, b)
    .then(() => cb())
    .catch(cb)
}

function loadTypedArrayFile(filename, Type, cb) {
  readFile(filename)
    .then((buf) => {
      const version = buf.readUInt32LE(0)
      const offset = buf.readUInt32LE(FIELD_SIZE)
      const count = buf.readUInt32LE(2 * FIELD_SIZE)
      const body = buf.slice(4 * FIELD_SIZE)

      cb(null, {
        version,
        offset,
        count,
        tarr: new Type(
          body.buffer,
          body.offset,
          body.byteLength / (Type === Float64Array ? 8 : 4)
        ),
      })
    })
    .catch(cb)
}

function savePrefixMapFile(filename, version, offset, count, map, cb) {
  if (!cb)
    cb = (err) => {
      if (err) console.error(err)
    }

  const jsonMap = JSON.stringify(map)
  const b = Buffer.alloc(4 * FIELD_SIZE + jsonMap.length)
  b.writeUInt32LE(version, 0)
  b.writeUInt32LE(offset, FIELD_SIZE)
  b.writeUInt32LE(count, 2 * FIELD_SIZE)
  Buffer.from(jsonMap).copy(b, 4 * FIELD_SIZE)

  writeFile(filename, b)
    .then(() => cb())
    .catch(cb)
}

function loadPrefixMapFile(filename, cb) {
  readFile(filename)
    .then((buf) => {
      const version = buf.readUInt32LE(0)
      const offset = buf.readUInt32LE(FIELD_SIZE)
      const count = buf.readUInt32LE(2 * FIELD_SIZE)
      const body = buf.slice(4 * FIELD_SIZE)
      const map = JSON.parse(body)

      cb(null, {
        version,
        offset,
        count,
        map,
      })
    })
    .catch(cb)
}

function saveBitsetFile(filename, version, offset, bitset, cb) {
  bitset.trim()
  const count = bitset.words.length
  saveTypedArrayFile(filename, version, offset, count, bitset.words, cb)
}

function loadBitsetFile(filename, cb) {
  loadTypedArrayFile(filename, Uint32Array, (err, data) => {
    if (err) cb(err)
    else {
      const { version, offset, count, tarr } = data
      const bitset = new TypedFastBitSet()
      bitset.words = tarr
      cb(null, { version, offset, bitset })
    }
  })
}

function listFilesIDB(dir, cb) {
  const IdbKvStore = require('idb-kv-store')
  const store = new IdbKvStore(dir, { disableBroadcast: true })
  store.keys(cb)
}

function listFilesFS(dir, cb) {
  const fs = require('fs')
  const mkdirp = require('mkdirp')
  mkdirp(dir).then(() => {
    fs.readdir(dir, cb)
  }, cb)
}

function safeFilename(filename) {
  // in general we want to escape wierd characters
  let result = jsesc(filename)
  // sanitize will remove special characters, which means that two
  // indexes might end up with the same name so lets replace those
  // with jsesc escapeEverything values
  result = result.replace(/\./g, 'x2E')
  result = result.replace(/\//g, 'x2F')
  result = result.replace(/\?/g, 'x3F')
  result = result.replace(/\</g, 'x3C')
  result = result.replace(/\>/g, 'x3E')
  result = result.replace(/\:/g, 'x3A')
  result = result.replace(/\*/g, 'x2A')
  result = result.replace(/\|/g, 'x7C')
  // finally sanitize
  return sanitize(result)
}

module.exports = {
  saveTypedArrayFile,
  loadTypedArrayFile,
  savePrefixMapFile,
  loadPrefixMapFile,
  saveBitsetFile,
  loadBitsetFile,
  listFilesIDB,
  listFilesFS,
  safeFilename,
}
