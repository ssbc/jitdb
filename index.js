// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const path = require('path')
const bipf = require('bipf')
const push = require('push-stream')
const pull = require('pull-stream')
const toPull = require('push-stream-to-pull-stream')
const pullAsync = require('pull-async')
const TypedFastBitSet = require('typedfastbitset')
const bsb = require('binary-search-bounds')
const multicb = require('multicb')
const FastPriorityQueue = require('fastpriorityqueue')
const debug = require('debug')('jitdb')
const debugQuery = debug.extend('query')
const Status = require('./status')
const {
  saveTypedArrayFile,
  loadTypedArrayFile,
  savePrefixMapFile,
  loadPrefixMapFile,
  saveBitsetFile,
  loadBitsetFile,
  safeFilename,
  listFilesIDB,
  listFilesFS,
} = require('./files')

module.exports = function (log, indexesPath) {
  debug('indexes path', indexesPath)

  let bitsetCache = new WeakMap()
  let sortedCache = { ascending: new WeakMap(), descending: new WeakMap() }
  let cacheOffset = -1

  const status = Status()

  const indexes = {}
  let isReady = false
  let waiting = []
  const coreIndexNames = ['seq', 'timestamp', 'sequence']

  loadIndexes(() => {
    debug('loaded indexes', Object.keys(indexes))

    if (!indexes['seq']) {
      indexes['seq'] = {
        offset: -1,
        count: 0,
        tarr: new Uint32Array(16 * 1000),
        version: 1,
      }
    }
    if (!indexes['timestamp']) {
      indexes['timestamp'] = {
        offset: -1,
        count: 0,
        tarr: new Float64Array(16 * 1000),
        version: 1,
      }
    }
    if (!indexes['sequence']) {
      indexes['sequence'] = {
        offset: -1,
        count: 0,
        tarr: new Uint32Array(16 * 1000),
        version: 1,
      }
    }

    status.batchUpdate(indexes, coreIndexNames)

    isReady = true
    for (let i = 0; i < waiting.length; ++i) waiting[i]()
    waiting = []
  })

  function onReady(cb) {
    if (isReady) cb()
    else waiting.push(cb)
  }

  const B_TIMESTAMP = Buffer.from('timestamp')
  const B_SEQUENCE = Buffer.from('sequence')
  const B_VALUE = Buffer.from('value')

  function loadIndexes(cb) {
    function parseIndexes(err, files) {
      push(
        push.values(files),
        push.asyncMap((file, cb) => {
          const indexName = path.parse(file).name
          if (file === 'seq.index') {
            loadTypedArrayFile(
              path.join(indexesPath, file),
              Uint32Array,
              (err, idx) => {
                if (!err) indexes[indexName] = idx
                cb()
              }
            )
          } else if (file === 'timestamp.index') {
            loadTypedArrayFile(
              path.join(indexesPath, file),
              Float64Array,
              (err, idx) => {
                if (!err) indexes[indexName] = idx
                cb()
              }
            )
          } else if (file === 'sequence.index') {
            loadTypedArrayFile(
              path.join(indexesPath, file),
              Uint32Array,
              (err, idx) => {
                if (!err) indexes[indexName] = idx
                cb()
              }
            )
          } else if (file.endsWith('.32prefix')) {
            // Don't load it yet, just tag it `lazy`
            indexes[indexName] = {
              offset: -1,
              count: 0,
              tarr: new Uint32Array(16 * 1000),
              lazy: true,
              prefix: 32,
              filepath: path.join(indexesPath, file),
            }
            cb()
          } else if (file.endsWith('.32prefixmap')) {
            // Don't load it yet, just tag it `lazy`
            indexes[indexName] = {
              offset: -1,
              count: 0,
              map: {},
              lazy: true,
              prefix: 32,
              filepath: path.join(indexesPath, file),
            }
            cb()
          } else if (file.endsWith('.index')) {
            // Don't load it yet, just tag it `lazy`
            indexes[indexName] = {
              offset: 0,
              bitset: new TypedFastBitSet(),
              lazy: true,
              filepath: path.join(indexesPath, file),
            }
            cb()
          } else cb()
        }),
        push.collect(cb)
      )
    }

    if (typeof window !== 'undefined') {
      // browser
      listFilesIDB(indexesPath, parseIndexes)
    } else {
      // node.js
      listFilesFS(indexesPath, parseIndexes)
    }
  }

  function updateCacheWithLog() {
    if (log.since.value > cacheOffset) {
      cacheOffset = log.since.value
      bitsetCache = new WeakMap()
      sortedCache.ascending = new WeakMap()
      sortedCache.descending = new WeakMap()
    }
  }

  function saveCoreIndex(name, coreIndex, count) {
    if (coreIndex.offset < 0) return
    debug('saving core index: %s', name)
    const filename = path.join(indexesPath, name + '.index')
    saveTypedArrayFile(
      filename,
      coreIndex.version,
      coreIndex.offset,
      count,
      coreIndex.tarr
    )
  }

  function saveIndex(name, index, count, cb) {
    if (index.prefix && index.map) savePrefixMapIndex(name, index, count, cb)
    else if (index.prefix) savePrefixIndex(name, index, count, cb)
    else saveBitsetIndex(name, index, cb)
  }

  function saveBitsetIndex(name, index, cb) {
    if (index.offset < 0 || index.bitset.size() === 0) return
    debug('saving index: %s', name)
    const filename = path.join(indexesPath, name + '.index')
    saveBitsetFile(filename, index.version, index.offset, index.bitset, cb)
  }

  function savePrefixIndex(name, prefixIndex, count, cb) {
    if (prefixIndex.offset < 0) return
    debug('saving prefix index: %s', name)
    const num = prefixIndex.prefix
    const filename = path.join(indexesPath, name + `.${num}prefix`)
    saveTypedArrayFile(
      filename,
      prefixIndex.version,
      prefixIndex.offset,
      count,
      prefixIndex.tarr,
      cb
    )
  }

  function savePrefixMapIndex(name, prefixIndex, count, cb) {
    if (prefixIndex.offset < 0) return
    debug('saving prefix map index: %s', name)
    const num = prefixIndex.prefix
    const filename = path.join(indexesPath, name + `.${num}prefixmap`)
    savePrefixMapFile(
      filename,
      prefixIndex.version,
      prefixIndex.offset,
      count,
      prefixIndex.map,
      cb
    )
  }

  function growTarrIndex(index, Type) {
    debug('growing index')
    const newArray = new Type(index.tarr.length * 2)
    newArray.set(index.tarr)
    index.tarr = newArray
  }

  function updateSeqIndex(seq, offset) {
    if (seq > indexes['seq'].count - 1) {
      if (seq > indexes['seq'].tarr.length - 1) {
        growTarrIndex(indexes['seq'], Uint32Array)
      }

      indexes['seq'].offset = offset
      indexes['seq'].tarr[seq] = offset
      indexes['seq'].count = seq + 1
      return true
    }
  }

  function seekMinTimestamp(buffer) {
    var p = 0 // note you pass in p!
    p = bipf.seekKey(buffer, p, B_TIMESTAMP)
    const arrivalTimestamp = bipf.decode(buffer, p)
    p = 0
    p = bipf.seekKey(buffer, p, B_VALUE)
    p = bipf.seekKey(buffer, p, B_TIMESTAMP)
    const declaredTimestamp = bipf.decode(buffer, p)
    return Math.min(arrivalTimestamp, declaredTimestamp)
  }

  function seekSequence(buffer) {
    var p = 0 // note you pass in p!
    p = bipf.seekKey(buffer, p, B_VALUE)
    p = bipf.seekKey(buffer, p, B_SEQUENCE)
    return bipf.decode(buffer, p)
  }

  function updateTimestampIndex(seq, offset, buffer) {
    if (seq > indexes['timestamp'].count - 1) {
      if (seq > indexes['timestamp'].tarr.length - 1)
        growTarrIndex(indexes['timestamp'], Float64Array)

      indexes['timestamp'].offset = offset

      const timestamp = seekMinTimestamp(buffer)

      indexes['timestamp'].tarr[seq] = timestamp
      indexes['timestamp'].count = seq + 1
      return true
    }
  }

  function updateSequenceIndex(seq, offset, buffer) {
    if (seq > indexes['sequence'].count - 1) {
      if (seq > indexes['sequence'].tarr.length - 1)
        growTarrIndex(indexes['sequence'], Uint32Array)

      indexes['sequence'].offset = offset

      const sequence = seekSequence(buffer)

      indexes['sequence'].tarr[seq] = sequence
      indexes['sequence'].count = seq + 1
      return true
    }
  }

  const undefinedBuffer = bipf.slice(bipf.allocAndEncode(undefined), 0)
  const nullBipf = bipf.allocAndEncode(null)

  function checkEqual(opData, buffer) {
    const fieldStart = opData.seek(buffer)

    // works for undefined
    if (fieldStart === -1) return opData.value.equals(undefinedBuffer)
    else if (opData.value.length === 0)
      return bipf.compare(buffer, fieldStart, nullBipf, 0) === 0
    else return bipf.compareValue(buffer, fieldStart, opData.value, 0) === 0
  }

  function compareWithRangeOp(op, value) {
    if (op.type === 'GT') return value > op.data.value
    else if (op.type === 'GTE') return value >= op.data.value
    else if (op.type === 'LT') return value < op.data.value
    else if (op.type === 'LTE') return value <= op.data.value
    else {
      console.warn('Unknown op type: ' + op.type)
      return true
    }
  }

  function checkComparison(op, buffer) {
    if (op.data.indexName === 'timestamp') {
      const timestamp = seekMinTimestamp(buffer)
      return compareWithRangeOp(op, timestamp)
    } else if (op.data.indexName === 'sequence') {
      const sequence = seekSequence(buffer)
      return compareWithRangeOp(op, sequence)
    } else {
      console.warn(
        `Attempted to do a ${op.type} comparison on unsupported index ${op.data.indexName}`
      )
      return true
    }
  }

  function checkPredicate(opData, buffer) {
    const fieldStart = opData.seek(buffer)
    const predicateFn = opData.value
    if (fieldStart < 0) return false
    const fieldValue = bipf.decode(buffer, fieldStart)
    return predicateFn(fieldValue)
  }

  function checkIncludes(opData, buffer) {
    const fieldStart = opData.seek(buffer)
    if (!~fieldStart) return false
    const type = bipf.getEncodedType(buffer, fieldStart)
    if (type === bipf.types.string) {
      return checkEqual(opData, buffer)
    } else if (type === bipf.types.array) {
      let found = false
      bipf.iterate(buffer, fieldStart, (_, itemStart) => {
        const valueStart = opData.pluck
          ? opData.pluck(buffer, itemStart)
          : itemStart
        if (bipf.compareString(buffer, valueStart, opData.value) === 0) {
          found = true
          return true // abort the bipf.iterate
        }
      })
      return found
    } else {
      return false
    }
  }

  function safeReadUint32(buf, prefixOffset = 0) {
    if (buf.length < 4) {
      const bigger = Buffer.alloc(4)
      buf.copy(bigger)
      return bigger.readUInt32LE(0)
    } else if (buf.length === 4) {
      return buf.readUInt32LE(0)
    } else {
      return buf.readUInt32LE(prefixOffset)
    }
  }

  function addToPrefixMap(map, seq, prefix) {
    if (prefix === 0) return

    const arr = map[prefix] || (map[prefix] = [])
    arr.push(seq)
  }

  function updatePrefixMapIndex(opData, index, buffer, seq, offset) {
    if (seq > index.count - 1) {
      const fieldStart = opData.seek(buffer)
      if (~fieldStart) {
        const buf = bipf.slice(buffer, fieldStart)
        if (buf.length) {
          const prefix = safeReadUint32(buf, opData.prefixOffset)
          addToPrefixMap(index.map, seq, prefix)
        }
      }

      index.offset = offset
      index.count = seq + 1
    }
  }

  function updatePrefixIndex(opData, index, buffer, seq, offset) {
    if (seq > index.count - 1) {
      if (seq > index.tarr.length - 1) growTarrIndex(index, Uint32Array)

      const fieldStart = opData.seek(buffer)
      if (~fieldStart) {
        const buf = bipf.slice(buffer, fieldStart)
        index.tarr[seq] = buf.length
          ? safeReadUint32(buf, opData.prefixOffset)
          : 0
      } else {
        index.tarr[seq] = 0
      }
      index.offset = offset
      index.count = seq + 1
    }
  }

  function updateIndexValue(op, index, buffer, seq) {
    if (op.type === 'EQUAL' && checkEqual(op.data, buffer))
      index.bitset.add(seq)
    else if (op.type === 'PREDICATE' && checkPredicate(op.data, buffer))
      index.bitset.add(seq)
    else if (op.type === 'INCLUDES' && checkIncludes(op.data, buffer))
      index.bitset.add(seq)
  }

  function updateAllIndexValue(opData, newIndexes, buffer, seq) {
    const fieldStart = opData.seek(buffer)
    const value = bipf.decode(buffer, fieldStart)
    const indexName = safeFilename(opData.indexType + '_' + value)

    if (!newIndexes[indexName]) {
      newIndexes[indexName] = {
        offset: 0,
        bitset: new TypedFastBitSet(),
        version: opData.version || 1,
      }
    }

    newIndexes[indexName].bitset.add(seq)
  }

  // concurrent index helpers
  function onlyOneIndexAtATime(waitingMap, indexName, cb) {
    if (waitingMap.has(indexName)) {
      waitingMap.get(indexName).push(cb)
      return true // wait for other index update
    } else waitingMap.set(indexName, [])
  }

  function runWaitingIndexLoadCbs(waitingMap, indexName) {
    waitingMap.get(indexName).forEach((cb) => cb())
    waitingMap.delete(indexName)
  }

  // concurrent index update
  const waitingIndexUpdate = new Map()

  function updateIndex(op, cb) {
    const index = indexes[op.data.indexName]

    const indexNamesForStatus = [...coreIndexNames, op.data.indexName]

    const waitingKey = op.data.indexName
    if (onlyOneIndexAtATime(waitingIndexUpdate, waitingKey, cb)) return

    // Reset index if version was bumped
    if (op.data.version > index.version) {
      index.offset = -1
      index.count = 0
    }

    // find the next possible seq
    let seq = 0
    if (index.offset !== -1) {
      const { tarr } = indexes['seq']
      const indexOffset = index.offset
      for (const len = tarr.length; seq < len; ++seq)
        if (tarr[seq] === indexOffset) {
          seq++
          break
        }
    }

    let updatedSeqIndex = false
    let updatedTimestampIndex = false
    let updatedSequenceIndex = false
    const startSeq = seq
    const start = Date.now()
    let lastSaved = start

    const indexNeedsUpdate = !coreIndexNames.includes(op.data.indexName)

    function save(count, offset) {
      if (updatedSeqIndex) saveCoreIndex('seq', indexes['seq'], count)

      if (updatedTimestampIndex)
        saveCoreIndex('timestamp', indexes['timestamp'], count)

      if (updatedSequenceIndex)
        saveCoreIndex('sequence', indexes['sequence'], count)

      index.offset = offset
      if (op.data.version > index.version) index.version = op.data.version

      if (indexNeedsUpdate) saveIndex(op.data.indexName, index, count)
    }

    const logstreamId = Math.ceil(Math.random() * 1000)
    debug(`log.stream #${logstreamId} started, to update index ${waitingKey}`)

    log.stream({ gt: index.offset }).pipe({
      paused: false,
      write: function (record) {
        const offset = record.offset
        const buffer = record.value

        if (updateSeqIndex(seq, offset)) updatedSeqIndex = true

        if (!buffer) {
          // deleted
          seq++
          return
        }

        if (updateTimestampIndex(seq, offset, buffer))
          updatedTimestampIndex = true

        if (updateSequenceIndex(seq, offset, buffer))
          updatedSequenceIndex = true

        if (indexNeedsUpdate) {
          if (op.data.prefix && op.data.useMap)
            updatePrefixMapIndex(op.data, index, buffer, seq, offset)
          else if (op.data.prefix)
            updatePrefixIndex(op.data, index, buffer, seq, offset)
          else updateIndexValue(op, index, buffer, seq)
        }

        if (seq % 1000 === 0) {
          status.batchUpdate(indexes, indexNamesForStatus)
          const now = Date.now()
          if (now - lastSaved >= 60e3) {
            lastSaved = now
            save(seq, offset)
          }
        }

        seq++
      },
      end: () => {
        const count = seq // incremented at end
        debug(
          `log.stream #${logstreamId} done ${seq - startSeq} records in ${
            Date.now() - start
          }ms`
        )

        save(count, indexes['seq'].offset)

        status.batchUpdate(indexes, indexNamesForStatus)

        runWaitingIndexLoadCbs(waitingIndexUpdate, waitingKey)

        cb()
      },
    })
  }

  // concurrent index create
  const waitingIndexCreate = new Map()

  function createIndexes(opsMissingIdx, cb) {
    const newIndexes = {}

    const newIndexNames = opsMissingIdx.map((op) => op.data.indexName)

    const waitingKey = newIndexNames.join('|')
    if (onlyOneIndexAtATime(waitingIndexCreate, waitingKey, cb)) return

    opsMissingIdx.forEach((op) => {
      if (op.data.prefix && op.data.useMap) {
        newIndexes[op.data.indexName] = {
          offset: 0,
          count: 0,
          map: {},
          prefix: typeof op.data.prefix === 'number' ? op.data.prefix : 32,
          version: op.data.version || 1,
        }
      } else if (op.data.prefix)
        newIndexes[op.data.indexName] = {
          offset: 0,
          count: 0,
          tarr: new Uint32Array(16 * 1000),
          prefix: typeof op.data.prefix === 'number' ? op.data.prefix : 32,
          version: op.data.version || 1,
        }
      else
        newIndexes[op.data.indexName] = {
          offset: 0,
          bitset: new TypedFastBitSet(),
          version: op.data.version || 1,
        }
    })

    let seq = 0

    let updatedSeqIndex = false
    let updatedTimestampIndex = false
    let updatedSequenceIndex = false
    const start = Date.now()
    let lastSaved = start

    function save(count, offset, done) {
      if (updatedSeqIndex) saveCoreIndex('seq', indexes['seq'], count)

      if (updatedTimestampIndex)
        saveCoreIndex('timestamp', indexes['timestamp'], count)

      if (updatedSequenceIndex)
        saveCoreIndex('sequence', indexes['sequence'], count)

      for (var indexName in newIndexes) {
        const index = newIndexes[indexName]
        if (done) indexes[indexName] = index
        index.offset = offset
        saveIndex(indexName, index, count)
      }
    }

    const logstreamId = Math.ceil(Math.random() * 1000)
    debug(`log.stream #${logstreamId} started, to create indexes ${waitingKey}`)

    log.stream({}).pipe({
      paused: false,
      write: function (record) {
        const offset = record.offset
        const buffer = record.value

        if (updateSeqIndex(seq, offset)) updatedSeqIndex = true

        if (!buffer) {
          // deleted
          seq++
          return
        }

        if (updateTimestampIndex(seq, offset, buffer))
          updatedTimestampIndex = true

        if (updateSequenceIndex(seq, offset, buffer))
          updatedSequenceIndex = true

        opsMissingIdx.forEach((op) => {
          if (op.data.prefix && op.data.useMap)
            updatePrefixMapIndex(
              op.data,
              newIndexes[op.data.indexName],
              buffer,
              seq,
              offset
            )
          else if (op.data.prefix)
            updatePrefixIndex(
              op.data,
              newIndexes[op.data.indexName],
              buffer,
              seq,
              offset
            )
          else if (op.data.indexAll)
            updateAllIndexValue(op.data, newIndexes, buffer, seq)
          else updateIndexValue(op, newIndexes[op.data.indexName], buffer, seq)
        })

        if (seq % 1000 === 0) {
          status.batchUpdate(indexes, coreIndexNames)
          status.batchUpdate(newIndexes, newIndexNames)
          const now = Date.now()
          if (now - lastSaved >= 60e3) {
            lastSaved = now
            save(seq, offset, false)
          }
        }

        seq++
      },
      end: () => {
        const count = seq // incremented at end
        debug(
          `log.stream #${logstreamId} done ${count} records in ${
            Date.now() - start
          }ms`
        )

        save(count, indexes['seq'].offset, true)

        status.batchUpdate(indexes, coreIndexNames)
        status.batchUpdate(newIndexes, newIndexNames)

        runWaitingIndexLoadCbs(waitingIndexCreate, waitingKey)

        cb()
      },
    })
  }

  // concurrent index load
  const waitingIndexLoad = new Map()

  function loadLazyIndex(indexName, cb) {
    if (onlyOneIndexAtATime(waitingIndexLoad, indexName, cb)) return

    debug('lazy loading %s', indexName)
    let index = indexes[indexName]
    if (index.prefix && index.map) {
      loadPrefixMapFile(index.filepath, (err, data) => {
        if (err) {
          debug('index %s failed to load with %s', indexName, err)
          delete indexes[indexName]
          return cb() // don't return a error, index will be rebuild
        }

        const { version, offset, count, map } = data
        index.version = version
        index.offset = offset
        index.count = count
        index.map = map
        index.lazy = false

        runWaitingIndexLoadCbs(waitingIndexLoad, indexName)

        cb()
      })
    } else if (index.prefix) {
      loadTypedArrayFile(index.filepath, Uint32Array, (err, data) => {
        if (err) {
          debug('index %s failed to load with %s', indexName, err)
          delete indexes[indexName]
          return cb() // don't return a error, index will be rebuild
        }

        const { version, offset, count, tarr } = data
        index.version = version
        index.offset = offset
        index.count = count
        index.tarr = tarr
        index.lazy = false

        runWaitingIndexLoadCbs(waitingIndexLoad, indexName)

        cb()
      })
    } else {
      loadBitsetFile(index.filepath, (err, data) => {
        if (err) {
          debug('index %s failed to load with %s', indexName, err)
          delete indexes[indexName]
          return cb() // don't return a error, index will be rebuild
        }

        const { version, offset, bitset } = data
        index.version = version
        index.offset = offset
        index.bitset = bitset
        index.lazy = false

        runWaitingIndexLoadCbs(waitingIndexLoad, indexName)

        cb()
      })
    }
  }

  function ensureIndexSync(op, cb) {
    const index = indexes[op.data.indexName]
    if (log.since.value > index.offset || op.data.version > index.version) {
      updateIndex(op, cb)
    } else {
      debug('ensureIndexSync %s is already synced', op.data.indexName)
      cb()
    }
  }

  function ensureSeqIndexSync(cb) {
    ensureIndexSync({ data: { indexName: 'seq' } }, cb)
  }

  function filterIndex(op, filterCheck, cb) {
    ensureIndexSync(op, () => {
      if (op.data.indexName === 'sequence') {
        const bitset = new TypedFastBitSet()
        const { tarr, count } = indexes['sequence']
        for (let seq = 0; seq < count; ++seq) {
          if (filterCheck(tarr[seq], op)) bitset.add(seq)
        }
        cb(bitset)
      } else if (op.data.indexName === 'timestamp') {
        const bitset = new TypedFastBitSet()
        const { tarr, count } = indexes['timestamp']
        for (let seq = 0; seq < count; ++seq) {
          if (filterCheck(tarr[seq], op)) bitset.add(seq)
        }
        cb(bitset)
      } else {
        debug('filterIndex() is unsupported for %s', op.data.indexName)
      }
    })
  }

  function getFullBitset(cb) {
    ensureIndexSync({ data: { indexName: 'sequence' } }, () => {
      const bitset = new TypedFastBitSet()
      const { count } = indexes['sequence']
      bitset.addRange(0, count)
      cb(bitset)
    })
  }

  function getOffsetsBitset(opOffsets, cb) {
    const seqs = []
    opOffsets.sort((x, y) => x - y)
    const opOffsetsLen = opOffsets.length
    const { tarr } = indexes['seq']
    for (let seq = 0, len = tarr.length; seq < len; ++seq) {
      if (bsb.eq(opOffsets, tarr[seq]) !== -1) seqs.push(seq)
      if (seqs.length === opOffsetsLen) break
    }
    cb(new TypedFastBitSet(seqs))
  }

  function matchAgainstPrefix(op, prefixIndex, cb) {
    const target = op.data.value
    const targetPrefix = target
      ? safeReadUint32(target, op.data.prefixOffset)
      : 0
    const bitset = new TypedFastBitSet()
    const bitsetFilters = new Map()

    const seek = op.data.seek
    function checker(value) {
      if (!value) return false // deleted

      const fieldStart = seek(value)
      const candidate = bipf.slice(value, fieldStart)
      if (target) {
        if (Buffer.compare(candidate, target)) return false
      } else {
        if (~fieldStart) return false
      }

      return true
    }

    if (prefixIndex.map) {
      if (prefixIndex.map[targetPrefix]) {
        prefixIndex.map[targetPrefix].forEach((seq) => {
          bitset.add(seq)
          bitsetFilters.set(seq, [checker])
        })
      }
    } else {
      const count = prefixIndex.count
      const tarr = prefixIndex.tarr
      for (let seq = 0; seq < count; ++seq) {
        if (tarr[seq] === targetPrefix) {
          bitset.add(seq)
          bitsetFilters.set(seq, [checker])
        }
      }
    }

    cb(bitset, bitsetFilters)
  }

  function nestLargeOpsArray(ops, type) {
    let op = ops[0]
    ops.slice(1).forEach((rest) => {
      op = {
        type,
        data: [op, rest],
      }
    })
    return op
  }

  function getNameFromOperation(op) {
    if (
      op.type === 'EQUAL' ||
      op.type === 'INCLUDES' ||
      op.type === 'PREDICATE'
    ) {
      const value = op.data.value
        ? op.data.value.toString().substring(0, 10)
        : ''
      return `${op.data.indexType}(${value})`
    } else if (
      op.type === 'GT' ||
      op.type === 'GTE' ||
      op.type === 'LT' ||
      op.type === 'LTE'
    ) {
      const value = op.data.value
        ? op.data.value.toString().substring(0, 10)
        : ''
      return `${op.type}(${value})`
    } else if (op.type === 'SEQS') {
      return `SEQS(${op.seqs.toString().substring(0, 10)})`
    } else if (op.type === 'OFFSETS') {
      return `OFFSETS(${op.offsets.toString().substring(0, 10)})`
    } else if (op.type === 'LIVESEQS') {
      return `LIVESEQS()`
    } else if (op.type === 'AND') {
      if (op.data.length > 2) op = nestLargeOpsArray(op.data, 'AND')

      const op1name = getNameFromOperation(op.data[0])
      const op2name = getNameFromOperation(op.data[1])

      if (!op1name) return op2name
      if (!op2name) return op1name

      return `AND(${op1name},${op2name})`
    } else if (op.type === 'OR') {
      if (op.data.length > 2) op = nestLargeOpsArray(op.data, 'AND')

      const op1name = getNameFromOperation(op.data[0])
      const op2name = getNameFromOperation(op.data[1])

      if (!op1name) return op2name
      if (!op2name) return op1name

      return `OR(${op1name},${op2name})`
    } else if (op.type === 'NOT') {
      return `NOT(${getNameFromOperation(op.data[0])})`
    } else {
      return '*'
    }
  }

  function mergeFilters(filters1, filters2) {
    if (!filters1 && !filters2) return null
    else if (filters1 && !filters2) return filters1
    else if (!filters1 && filters2) return filters2
    else {
      const filters = new Map(filters1)
      for (let seq of filters2.keys()) {
        const f1 = filters1.get(seq) || []
        const f2 = filters2.get(seq)
        filters.set(seq, [...f1, ...f2])
      }
      return filters
    }
  }

  function getBitsetForOperation(op, cb) {
    if (
      op.type === 'EQUAL' ||
      op.type === 'INCLUDES' ||
      op.type === 'PREDICATE'
    ) {
      if (op.data.prefix) {
        ensureIndexSync(op, () => {
          matchAgainstPrefix(op, indexes[op.data.indexName], cb)
        })
      } else {
        ensureIndexSync(op, () => {
          cb(indexes[op.data.indexName].bitset)
        })
      }
    } else if (op.type === 'GT') {
      filterIndex(op, (num, op) => num > op.data.value, cb)
    } else if (op.type === 'GTE') {
      filterIndex(op, (num, op) => num >= op.data.value, cb)
    } else if (op.type === 'LT') {
      filterIndex(op, (num, op) => num < op.data.value, cb)
    } else if (op.type === 'LTE') {
      filterIndex(op, (num, op) => num <= op.data.value, cb)
    } else if (op.type === 'OFFSETS') {
      ensureSeqIndexSync(() => {
        getOffsetsBitset(op.offsets, cb)
      })
    } else if (op.type === 'SEQS') {
      ensureSeqIndexSync(() => {
        cb(new TypedFastBitSet(op.seqs))
      })
    } else if (op.type === 'LIVESEQS') {
      cb(new TypedFastBitSet())
    } else if (op.type === 'AND') {
      if (op.data.length > 2) op = nestLargeOpsArray(op.data, 'AND')

      getBitsetForOperation(op.data[0], (op1, filters1) => {
        getBitsetForOperation(op.data[1], (op2, filters2) => {
          cb(op1.new_intersection(op2), mergeFilters(filters1, filters2))
        })
      })
    } else if (op.type === 'OR') {
      if (op.data.length > 2) op = nestLargeOpsArray(op.data, 'OR')

      getBitsetForOperation(op.data[0], (op1, filters1) => {
        getBitsetForOperation(op.data[1], (op2, filters2) => {
          cb(op1.new_union(op2), mergeFilters(filters1, filters2))
        })
      })
    } else if (op.type === 'NOT') {
      getBitsetForOperation(op.data[0], (op1, filters) => {
        getFullBitset((fullBitset) => {
          cb(fullBitset.difference(op1), filters)
        })
      })
    } else if (!op.type) {
      // to support `query(fromDB(jitdb), toCallback(cb))`
      getFullBitset(cb)
    } else console.error('Unknown type', op)
  }

  function traverseEqualsAndIncludes(operation, fn) {
    function traverseMore(ops) {
      ops.forEach((op) => {
        if (
          op.type === 'EQUAL' ||
          op.type === 'INCLUDES' ||
          op.type === 'PREDICATE'
        ) {
          fn(op)
        } else if (op.type === 'AND' || op.type === 'OR' || op.type === 'NOT')
          traverseMore(op.data)
        else if (
          op.type === 'SEQS' ||
          op.type === 'LIVESEQS' ||
          op.type === 'OFFSETS' ||
          op.type === 'LT' ||
          op.type === 'LTE' ||
          op.type === 'GT' ||
          op.type === 'GTE' ||
          !op.type // e.g. query(fromDB, toCallback), or empty deferred()
        );
        else debug('Unknown operator type: ' + op.type)
      })
    }
    traverseMore([operation])
  }

  function detectLazyIndexesUsed(operation) {
    const results = []
    traverseEqualsAndIncludes(operation, (op) => {
      const name = op.data.indexName
      if (indexes[name] && indexes[name].lazy) results.push(name)
    })
    return results
  }

  function detectOpsMissingIndexes(operation) {
    const results = []
    traverseEqualsAndIncludes(operation, (op) => {
      if (!indexes[op.data.indexName]) results.push(op)
    })
    return results
  }

  function executeOperation(operation, cb) {
    updateCacheWithLog()
    if (bitsetCache.has(operation)) return cb(null, bitsetCache.get(operation))

    push(
      // kick-start this chain with a dummy null value
      push.values([null]),

      // ensure that the seq->offset index is synchronized with the log
      push.asyncMap((_, next) => ensureSeqIndexSync(next)),

      // load lazy indexes, if any
      push.asyncMap((_, next) => {
        const lazyIdxNames = detectLazyIndexesUsed(operation)
        if (lazyIdxNames.length === 0) return next()
        push(
          push.values(lazyIdxNames),
          push.asyncMap(loadLazyIndex),
          push.collect(next)
        )
      }),

      // create missing indexes, if any
      //
      // this needs to happen after loading lazy indexes because some
      // lazy indexes may have failed to load, and are now considered missing
      push.asyncMap((_, next) => {
        const opsMissingIdx = detectOpsMissingIndexes(operation)
        if (opsMissingIdx.length === 0) return next()
        debug('missing indexes: %o', opsMissingIdx)
        createIndexes(opsMissingIdx, next)
      }),

      // get bitset for the input operation, and cache it
      push.asyncMap((_, next) => {
        getBitsetForOperation(operation, (bitset, filters) => {
          bitsetCache.set(operation, [bitset, filters])
          next(null, [bitset, filters])
        })
      }),

      // return bitset and filter functions
      push.collect((err, results) => {
        if (err) cb(err)
        else cb(null, results[0])
      })
    )
  }

  function isValueOk(ops, value, isOr) {
    for (let i = 0; i < ops.length; ++i) {
      const op = ops[i]
      let ok = false
      if (op.type === 'EQUAL') ok = checkEqual(op.data, value)
      else if (op.type === 'PREDICATE') ok = checkPredicate(op.data, value)
      else if (op.type === 'INCLUDES') ok = checkIncludes(op.data, value)
      else if (op.type === 'NOT') ok = !isValueOk(op.data, value, false)
      else if (op.type === 'AND') ok = isValueOk(op.data, value, false)
      else if (op.type === 'OR') ok = isValueOk(op.data, value, true)
      else if (
        op.type === 'GT' ||
        op.type === 'GTE' ||
        op.type === 'LT' ||
        op.type === 'LTE'
      )
        ok = checkComparison(op, value)
      else if (op.type === 'LIVESEQS') ok = true
      else if (!op.type) ok = true

      if (ok && isOr) return true
      else if (!ok && !isOr) return false
    }

    if (isOr) return false
    else return true
  }

  function getMessage(seq, recBufferCache, cb) {
    if (recBufferCache[seq]) {
      const recBuffer = recBufferCache[seq]
      const message = bipf.decode(recBuffer, 0)
      cb(null, message)
      return
    }
    const offset = indexes['seq'].tarr[seq]
    log.get(offset, (err, recBuffer) => {
      if (err && err.code === 'flumelog:deleted') cb()
      else if (err) cb(err)
      else cb(null, bipf.decode(recBuffer, 0))
    })
  }

  function getRecord(seq, cb) {
    const offset = indexes['seq'].tarr[seq]
    log.get(offset, (err, value) => {
      if (err && err.code === 'flumelog:deleted') cb(null, { seq, offset })
      else if (err) cb(err)
      else cb(null, { offset, value, seq })
    })
  }

  function compareAscending(a, b) {
    return b.timestamp > a.timestamp
  }

  function compareDescending(a, b) {
    return a.timestamp > b.timestamp
  }

  function sortedByTimestamp(bitset, descending) {
    updateCacheWithLog()
    const order = descending ? 'descending' : 'ascending'
    if (sortedCache[order].has(bitset)) return sortedCache[order].get(bitset)
    const fpq = new FastPriorityQueue(
      descending ? compareDescending : compareAscending
    )
    bitset.forEach((seq) => {
      fpq.add({
        seq,
        timestamp: indexes['timestamp'].tarr[seq],
      })
    })
    fpq.trim()
    sortedCache[order].set(bitset, fpq)
    return fpq
  }

  function filterRecord(seq, filters, recBufferCache, cb) {
    const seqFilters = filters.get(seq)
    if (!seqFilters) return cb(null, seq)

    getRecord(seq, (err, record) => {
      if (err) return cb(err)

      const recBuffer = record.value
      let ok = true
      if (seqFilters) ok = seqFilters.every((filter) => filter(recBuffer))

      if (ok) {
        recBufferCache[seq] = recBuffer
        cb(null, seq)
      } else cb()
    })
  }

  function sliceResults(sorted, seq, limit) {
    if (sorted.size === 0 || limit <= 0) {
      return []
    } else if (seq === 0 && limit === 1) {
      return [sorted.peek()]
    } else {
      if (seq > 0) {
        sorted = sorted.clone()
        for (let j = 0; j < seq && !sorted.isEmpty(); j++) {
          sorted.poll()
        }
      }
      return sorted.kSmallest(limit || Infinity)
    }
  }

  function getMessagesFromBitsetSlice(
    bitset,
    filters,
    seq,
    limit,
    descending,
    onlyOffset,
    cb
  ) {
    seq = seq || 0

    const sorted = sortedByTimestamp(bitset, descending)
    const resultSize = sorted.size

    // seq -> record buffer
    const recBufferCache = {}

    function processResults(seqs, resultSize) {
      push(
        push.values(seqs),
        push.asyncMap((seq, continueCB) => {
          if (onlyOffset) continueCB(null, indexes['seq'].tarr[seq])
          else getMessage(seq, recBufferCache, continueCB)
        }),
        push.filter((x) => (onlyOffset ? true : x)), // removes deleted messages
        push.collect((err, results) => {
          cb(err, {
            results: results,
            total: resultSize,
          })
        })
      )
    }

    if (filters) {
      function ensureEnoughResults(err, startSeq, seqs) {
        if (err) return cb(err)
        const rawLength = seqs.length
        seqs = seqs.filter((x) => x !== undefined)
        const moreResults = startSeq + seqs.length < sorted.size

        if (seqs.length < limit && moreResults)
          // results were filtered or deleted
          getMoreResults(
            startSeq + rawLength,
            limit - seqs.length,
            seqs,
            (e, seqs) => ensureEnoughResults(e, startSeq + rawLength, seqs)
          )
        else processResults(seqs, resultSize)
      }

      function getMoreResults(startSeq, remaining, seqs, continueCB) {
        const done = multicb({ pluck: 1 })

        // existing results
        for (let i = 0; i < seqs.length; ++i) done()(null, seqs[i])

        const sliced = sliceResults(sorted, startSeq, remaining)
        for (let i = 0; i < sliced.length; ++i)
          filterRecord(sliced[i].seq, filters, recBufferCache, done())

        done(continueCB)
      }

      getMoreResults(seq, limit, [], (err, seqs) =>
        ensureEnoughResults(err, seq, seqs)
      )
    } else {
      const slicedSeqs = sliceResults(sorted, seq, limit).map((s) => s.seq)
      processResults(slicedSeqs, resultSize)
    }
  }

  function countBitsetSlice(bitset, seq, descending) {
    if (!seq) return bitset.size()
    else return bitset.size() - seq
  }

  function paginate(operation, seq, limit, descending, onlyOffset, cb) {
    onReady(() => {
      const start = Date.now()
      executeOperation(operation, (err0, result) => {
        if (err0) return cb(err0)
        const [bitset, filters] = result
        getMessagesFromBitsetSlice(
          bitset,
          filters,
          seq,
          limit,
          descending,
          onlyOffset,
          (err1, answer) => {
            if (err1) cb(err1)
            else {
              answer.duration = Date.now() - start
              if (debugQuery.enabled)
                debugQuery(
                  `paginate(${getNameFromOperation(
                    operation
                  )}), seq: ${seq}, limit: ${limit}: ${
                    answer.duration
                  }ms, total messages: ${answer.total}`.replace(/%/g, '%% ')
                )
              cb(null, answer)
            }
          }
        )
      })
    })
  }

  function all(operation, seq, descending, onlyOffset, cb) {
    onReady(() => {
      const start = Date.now()
      executeOperation(operation, (err0, result) => {
        if (err0) return cb(err0)
        const [bitset, filters] = result
        getMessagesFromBitsetSlice(
          bitset,
          filters,
          seq,
          Infinity,
          descending,
          onlyOffset,
          (err1, answer) => {
            if (err1) cb(err1)
            else {
              answer.duration = Date.now() - start
              if (debugQuery.enabled)
                debugQuery(
                  `all(${getNameFromOperation(operation)}): ${
                    answer.duration
                  }ms, total messages: ${answer.total}`.replace(/%/g, '%% ')
                )
              cb(null, answer.results)
            }
          }
        )
      })
    })
  }

  function count(operation, seq, descending, cb) {
    onReady(() => {
      const start = Date.now()
      executeOperation(operation, (err0, result) => {
        if (err0) return cb(err0)
        const [bitset] = result
        const total = countBitsetSlice(bitset, seq, descending)
        const duration = Date.now() - start
        if (debugQuery.enabled)
          debugQuery(
            `count(${getNameFromOperation(
              operation
            )}): ${duration}ms, total messages: ${total}`.replace(/%/g, '%% ')
          )
        cb(null, total)
      })
    })
  }

  // live will return new messages as they enter the log
  // can be combined with a normal all or paginate first
  function live(op) {
    return pull(
      pullAsync((cb) =>
        onReady(() => {
          executeOperation(op, (err) => cb(err))
        })
      ),
      pull.map(() => {
        let offset = -1
        let seqStream

        function detectOffsetAndSeqStream(ops) {
          ops.forEach((op) => {
            if (
              op.type === 'EQUAL' ||
              op.type === 'INCLUDES' ||
              op.type === 'PREDICATE'
            ) {
              if (!indexes[op.data.indexName]) offset = -1
              else offset = indexes[op.data.indexName].offset
            } else if (
              op.type === 'AND' ||
              op.type === 'OR' ||
              op.type === 'NOT'
            ) {
              detectOffsetAndSeqStream(op.data)
            } else if (op.type === 'LIVESEQS') {
              if (seqStream)
                throw new Error('Only one seq stream in live supported')
              seqStream = op.stream
            }
          })
        }

        detectOffsetAndSeqStream([op])

        // There are two cases here:
        // - op contains a live seq stream, in which case we let the
        //   seq stream drive new values
        // - op doesn't, in which we let the log stream drive new values

        let recordStream
        if (seqStream) {
          recordStream = pull(
            seqStream,
            pull.asyncMap((seq, cb) => {
              ensureSeqIndexSync(() => {
                getRecord(seq, cb)
              })
            })
          )
        } else {
          const opts =
            offset === -1
              ? { live: true, gt: indexes['seq'].offset }
              : { live: true, gt: offset }
          const logstreamId = Math.ceil(Math.random() * 1000)
          debug(`log.stream #${logstreamId} started, for a live query`)
          recordStream = toPull(log.stream(opts))
        }

        return recordStream
      }),
      pull.flatten(),
      pull.filter((record) => isValueOk([op], record.value)),
      pull.through(() => {
        if (debugQuery.enabled)
          debugQuery(
            `live(${getNameFromOperation(op)}): 1 new msg`.replace(/%/g, '%% ')
          )
      }),
      pull.map((record) => bipf.decode(record.value, 0))
    )
  }

  function reindex(offset, cb) {
    // Find the previous offset and corresponding seq.
    // We need previous because log.stream() is always gt
    let seq = 0
    let prevOffset = 0
    if (offset === 0 || offset === -1) {
      prevOffset = -1
    } else {
      const { tarr } = indexes['seq']
      for (const len = tarr.length; seq < len; ++seq) {
        if (tarr[seq] === offset) break
        else prevOffset = tarr[seq]
      }
    }

    if (prevOffset === 0 && seq === indexes['seq'].tarr.length) {
      // not found
      seq = 1
    }

    function resetIndex(index) {
      if (index.offset >= prevOffset) {
        if (index.count) index.count = seq

        if (index.map) {
          // prefix map pushes to arrays, so we need to clean up
          for (let [prefix, arr] of Object.entries(index.map)) {
            index.map[prefix] = arr.filter((x) => x < seq)
          }
        }

        index.offset = prevOffset
      }
    }

    push(
      push.values(Object.entries(indexes)),
      push.asyncMap(([indexName, index], cb) => {
        if (coreIndexNames.includes(indexName)) return cb()

        if (index.lazy) {
          loadLazyIndex(indexName, (err) => {
            if (err) return cb(err)

            resetIndex(index)
            saveIndex(indexName, index, seq)
            cb()
          })
        } else {
          resetIndex(index)
          saveIndex(indexName, index, seq)
          cb()
        }
      }),
      push.collect((err) => {
        if (err) return cb(err)

        bitsetCache = new WeakMap()
        sortedCache.ascending = new WeakMap()
        sortedCache.descending = new WeakMap()
        cb()
      })
    )
  }

  return {
    onReady,
    paginate,
    all,
    count,
    live,
    status: status.obv,
    reindex,

    // testing
    indexes,
  }
}
