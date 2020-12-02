const jsesc = require('jsesc')
const sanitize = require('sanitize-filename')
const TypedFastBitSet = require('typedfastbitset')
const { readFile, writeFile } = require('atomically-universal')
const toBuffer = require('typedarray-to-buffer')

function saveTypedArrayFile(filename, seq, count, tarr, cb) {
  if (!cb) cb = () => {}

  const dataBuffer = toBuffer(tarr)
  const b = Buffer.alloc(8 + dataBuffer.length)
  b.writeInt32LE(seq, 0)
  b.writeInt32LE(count, 4)
  dataBuffer.copy(b, 8)

  writeFile(filename, b)
    .then(() => cb())
    .catch(cb)
}

function loadTypedArrayFile(filename, Type, cb) {
  readFile(filename)
    .then((buf) => {
      const seq = buf.readInt32LE(0)
      const count = buf.readInt32LE(4)
      const body = buf.slice(8)

      cb(null, {
        seq,
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

function saveBitsetFile(filename, seq, bitset, cb) {
  bitset.trim()
  saveTypedArrayFile(filename, seq, bitset.count, bitset.words, cb)
}

function loadBitsetFile(filename, cb) {
  loadTypedArrayFile(filename, Uint32Array, (err, { tarr, count, seq }) => {
    if (err) cb(err)
    else {
      const bitset = new TypedFastBitSet()
      bitset.words = tarr
      bitset.count = count
      cb(null, { seq, bitset })
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
  saveBitsetFile,
  loadBitsetFile,
  listFilesIDB,
  listFilesFS,
  safeFilename,
}
