// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const jsesc = require('jsesc')
const sanitize = require('sanitize-filename')
const TypedFastBitSet = require('typedfastbitset')
const { readFile, writeFile } = require('atomic-file-rw')
const toBuffer = require('typedarray-to-buffer')
const crcCalculate = require('crc/lib/crc32')

const FIELD_SIZE = 4 // bytes

/*
 * ## File format for tarr files
 *
 * Each header field is 4 bytes in size.
 *
 * | offset (bytes) | name    | type     |
 * | 0              | version | UInt32LE |
 * |----------------|---------|----------|
 * | 4              | offset  | UInt32LE |
 * | 8              | count   | UInt32LE |
 * | 12             | crc     | UInt32LE |
 * | 16             | body    | Buffer   |
 */

function calculateCRCAndWriteFile(buf, filename, cb) {
  try {
    const crc = crcCalculate(buf)
    buf.writeUInt32LE(crc, 3 * FIELD_SIZE)
  } catch (err) {
    return cb(err)
  }
  writeFile(filename, buf, cb)
}

function readFileAndCheckCRC(filename, cb) {
  readFile(filename, (err, buf) => {
    if (err) return cb(err)
    if (buf.length < 16) return cb(Error('file too short'))

    let crcFile
    let crc
    try {
      crcFile = buf.readUInt32LE(3 * FIELD_SIZE)
      buf.writeUInt32LE(0, 3 * FIELD_SIZE)
      crc = crcCalculate(buf)
    } catch (err) {
      return cb(err)
    }

    if (crcFile !== 0 && crc !== crcFile) return cb(Error('crc check failed'))
    cb(null, buf)
  })
}

function saveTypedArrayFile(filename, version, offset, count, tarr, cb) {
  if (!cb)
    cb = (err) => {
      if (err) console.error(err)
    }

  if (typeof version !== 'number') {
    return cb(Error('cannot save file ' + filename + ' without version'))
  }

  let buf
  try {
    const dataBuffer = toBuffer(tarr)
    // we try to save an extra 10% so we don't have to immediately grow
    // after loading and adding again
    const saveSize = Math.min(count * 1.1, tarr.length)
    buf = Buffer.alloc(4 * FIELD_SIZE + saveSize * tarr.BYTES_PER_ELEMENT)
    buf.writeUInt32LE(version, 0)
    buf.writeUInt32LE(offset, FIELD_SIZE)
    buf.writeUInt32LE(count, 2 * FIELD_SIZE)
    dataBuffer.copy(buf, 4 * FIELD_SIZE)
  } catch (err) {
    return cb(err)
  }

  calculateCRCAndWriteFile(buf, filename, cb)
}

function loadTypedArrayFile(filename, Type, cb) {
  readFileAndCheckCRC(filename, (err, buf) => {
    if (err) return cb(err)

    let version, offset, count, tarr
    try {
      version = buf.readUInt32LE(0)
      offset = buf.readUInt32LE(FIELD_SIZE)
      count = buf.readUInt32LE(2 * FIELD_SIZE)
      const body = buf.slice(4 * FIELD_SIZE)
      tarr = new Type(
        body.buffer,
        body.offset,
        body.byteLength / (Type === Float64Array ? 8 : 4)
      )
    } catch (err) {
      return cb(err)
    }

    cb(null, { version, offset, count, tarr })
  })
}

function savePrefixMapFile(filename, version, offset, count, map, cb) {
  if (!cb)
    cb = (err) => {
      if (err) console.error(err)
    }

  if (typeof version !== 'number') {
    return cb(Error('cannot save file ' + filename + ' without version'))
  }

  let buf
  try {
    const jsonMap = JSON.stringify(map)
    buf = Buffer.alloc(4 * FIELD_SIZE + jsonMap.length)
    buf.writeUInt32LE(version, 0)
    buf.writeUInt32LE(offset, FIELD_SIZE)
    buf.writeUInt32LE(count, 2 * FIELD_SIZE)
    Buffer.from(jsonMap).copy(buf, 4 * FIELD_SIZE)
  } catch (err) {
    return cb(err)
  }

  calculateCRCAndWriteFile(buf, filename, cb)
}

function loadPrefixMapFile(filename, cb) {
  readFileAndCheckCRC(filename, (err, buf) => {
    if (err) return cb(err)

    let version, offset, count, map
    try {
      version = buf.readUInt32LE(0)
      offset = buf.readUInt32LE(FIELD_SIZE)
      count = buf.readUInt32LE(2 * FIELD_SIZE)
      const body = buf.slice(4 * FIELD_SIZE)
      map = JSON.parse(body)
    } catch (err) {
      return cb(err)
    }

    cb(null, { version, offset, count, map })
  })
}

function saveBitsetFile(filename, version, offset, bitset, cb) {
  let count
  try {
    bitset.trim()
    count = bitset.words.length
  } catch (err) {
    return cb(err)
  }
  saveTypedArrayFile(filename, version, offset, count, bitset.words, cb)
}

function loadBitsetFile(filename, cb) {
  loadTypedArrayFile(filename, Uint32Array, (err, data) => {
    if (err) return cb(err)

    const { version, offset, count, tarr } = data
    const bitset = new TypedFastBitSet()
    bitset.words = tarr
    cb(null, { version, offset, bitset })
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
