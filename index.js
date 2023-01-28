// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const path = require('path')
const bipf = require('bipf')
const push = require('push-stream')
const pull = require('pull-stream')
const mutexify = require('mutexify')
const toPull = require('push-stream-to-pull-stream')
const pullAsync = require('pull-async')
const TypedFastBitSet = require('typedfastbitset')
const bsb = require('binary-search-bounds')
const multicb = require('multicb')
const FastPriorityQueue = require('fastpriorityqueue')
const Obv = require('obz')
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
  listFiles,
  EmptyFile,
} = require('./files')

module.exports = function (log, indexesPath) {
  debug('indexes path', indexesPath)

  let bitsetCache = new WeakMap()
  let sortedTSCache = { ascending: new WeakMap(), descending: new WeakMap() }
  let sortedSeqCache = { ascending: new WeakMap(), descending: new WeakMap() }
  let cacheOffset = -1

  const status = Status()

  const indexes = new Map() // indexName (string) -> Index (object)
  let seqIndex
  let timestampIndex
  let sequenceIndex
  let isReady = false
  let jitdb
  const waitingReady = new Set()
  let compacting = false
  let compactStartOffset = null
  const postCompactReindexPath = path.join(indexesPath, 'post-compact-reindex')
  const waitingCompaction = new Set()
  const coreIndexNames = ['seq', 'timestamp', 'sequence']
  const indexingActive = Obv().set(0)
  const queriesActive = Obv().set(0)

  loadIndexes(() => {
    debug('loaded indexes', [...indexes.keys()])

    if (!indexes.has('seq')) {
      indexes.set('seq', {
        offset: -1,
        count: 0,
        tarr: new Uint32Array(16 * 1000),
        version: 1,
      })
    }
    if (!indexes.has('timestamp')) {
      indexes.set('timestamp', {
        offset: -1,
        count: 0,
        tarr: new Float64Array(16 * 1000),
        version: 1,
      })
    }
    if (!indexes.has('sequence')) {
      indexes.set('sequence', {
        offset: -1,
        count: 0,
        tarr: new Uint32Array(16 * 1000),
        version: 1,
      })
    }

    seqIndex = indexes.get('seq')
    timestampIndex = indexes.get('timestamp')
    sequenceIndex = indexes.get('sequence')

    status.update(indexes, coreIndexNames)

    isReady = true
    for (const cb of waitingReady) cb()
    waitingReady.clear()
  })

  function onReady(cb) {
    if (isReady) cb()
    else waitingReady.add(cb)
  }

  log.compactionProgress((stats) => {
    if (typeof stats.startOffset === 'number' && compactStartOffset === null) {
      compactStartOffset = stats.startOffset
    }

    if (!stats.done && !compacting) {
      compacting = true
      EmptyFile.create(postCompactReindexPath)
    } else if (stats.done && compacting) {
      compacting = false
      const offset = compactStartOffset || 0
      compactStartOffset = null

      if (stats.sizeDiff > 0) {
        EmptyFile.exists(postCompactReindexPath, (err, exists) => {
          if (exists) {
            reindex(offset, (err) => {
              if (err) console.error('reindex jitdb after compact', err)
              conclude()
            })
          } else {
            conclude()
          }
        })
      } else {
        conclude()
      }

      function conclude() {
        EmptyFile.delete(postCompactReindexPath, () => {
          if (waitingCompaction.size === 0) return
          for (const cb of waitingCompaction) cb()
          waitingCompaction.clear()
        })
      }
    }
  })

  const BIPF_TIMESTAMP = bipf.allocAndEncode('timestamp')
  const BIPF_SEQUENCE = bipf.allocAndEncode('sequence')
  const BIPF_VALUE = bipf.allocAndEncode('value')
  const BIPF_KEY = bipf.allocAndEncode('key')

  function loadIndexes(cb) {
    listFiles(indexesPath, function parseIndexes(err, files) {
      push(
        push.values(files),
        push.asyncMap((file, cb) => {
          const indexName = path.parse(file).name
          if (file === 'seq.index') {
            loadTypedArrayFile(
              path.join(indexesPath, file),
              Uint32Array,
              (err, idx) => {
                if (!err) indexes.set(indexName, idx)
                cb()
              }
            )
          } else if (file === 'timestamp.index') {
            loadTypedArrayFile(
              path.join(indexesPath, file),
              Float64Array,
              (err, idx) => {
                if (!err) indexes.set(indexName, idx)
                cb()
              }
            )
          } else if (file === 'sequence.index') {
            loadTypedArrayFile(
              path.join(indexesPath, file),
              Uint32Array,
              (err, idx) => {
                if (!err) indexes.set(indexName, idx)
                cb()
              }
            )
          } else if (file.endsWith('.32prefix')) {
            // Don't load it yet, just tag it `lazy`
            indexes.set(indexName, {
              offset: -1,
              count: 0,
              tarr: new Uint32Array(16 * 1000),
              lazy: true,
              prefix: 32,
              filepath: path.join(indexesPath, file),
            })
            cb()
          } else if (file.endsWith('.32prefixmap')) {
            // Don't load it yet, just tag it `lazy`
            indexes.set(indexName, {
              offset: -1,
              count: 0,
              map: {},
              lazy: true,
              prefix: 32,
              filepath: path.join(indexesPath, file),
            })
            cb()
          } else if (file.endsWith('.index')) {
            // Don't load it yet, just tag it `lazy`
            indexes.set(indexName, {
              offset: 0,
              bitset: new TypedFastBitSet(),
              lazy: true,
              filepath: path.join(indexesPath, file),
            })
            cb()
          } else cb()
        }),
        push.drain(null, cb)
      )
    })
  }

  function clearCache() {
    bitsetCache = new WeakMap()
    sortedTSCache.ascending = new WeakMap()
    sortedTSCache.descending = new WeakMap()
    sortedSeqCache.ascending = new WeakMap()
    sortedSeqCache.descending = new WeakMap()
  }

  function updateCacheWithLog() {
    if (log.since.value > cacheOffset) {
      cacheOffset = log.since.value
      clearCache()
    }
  }

  function saveCoreIndex(name, coreIndex, count, cb) {
    if (coreIndex.offset < 0) return
    debug('saving core index: %s', name)
    const filename = path.join(indexesPath, name + '.index')
    saveTypedArrayFile(
      filename,
      coreIndex.version,
      coreIndex.offset,
      count,
      coreIndex.tarr,
      cb
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
    debug('growing index %s', index.name)
    const newArray = new Type(index.tarr.length * 2)
    newArray.set(index.tarr)
    index.tarr = newArray
  }

  function updateSeqIndex(seq, offset) {
    if (seq > seqIndex.count - 1) {
      if (seq > seqIndex.tarr.length - 1) {
        growTarrIndex(seqIndex, Uint32Array)
      }

      seqIndex.tarr[seq] = offset
      seqIndex.offset = offset
      seqIndex.count = seq + 1
      return true
    }
  }

  function seekMinTimestamp(buffer, pValue) {
    const pTimestamp = bipf.seekKey2(buffer, 0, BIPF_TIMESTAMP, 0)
    const arrivalTimestamp = bipf.decode(buffer, pTimestamp)
    const pValueTimestamp = bipf.seekKey2(buffer, pValue, BIPF_TIMESTAMP, 0)
    const declaredTimestamp = bipf.decode(buffer, pValueTimestamp)
    return Math.min(arrivalTimestamp, declaredTimestamp)
  }

  function seekSequence(buffer, pValue) {
    const pValueSequence = bipf.seekKey2(buffer, pValue, BIPF_SEQUENCE, 0)
    return bipf.decode(buffer, pValueSequence)
  }

  function updateTimestampIndex(seq, offset, buffer, pValue) {
    if (seq > timestampIndex.count - 1) {
      if (seq > timestampIndex.tarr.length - 1) {
        growTarrIndex(timestampIndex, Float64Array)
      }

      timestampIndex.tarr[seq] = seekMinTimestamp(buffer, pValue)
      timestampIndex.offset = offset
      timestampIndex.count = seq + 1
      return true
    }
  }

  function updateSequenceIndex(seq, offset, buffer, pValue) {
    if (seq > sequenceIndex.count - 1) {
      if (seq > sequenceIndex.tarr.length - 1) {
        growTarrIndex(sequenceIndex, Uint32Array)
      }

      sequenceIndex.tarr[seq] = seekSequence(buffer, pValue)
      sequenceIndex.offset = offset
      sequenceIndex.count = seq + 1
      return true
    }
  }

  function getSeqFromOffset(offset) {
    if (offset === -1) return -1
    const { tarr, count } = seqIndex
    if (tarr[count - 1] === offset) return count - 1
    const seq = bsb.eq(tarr, offset, 0, count - 1)
    if (seq < 0) return 0
    return seq
  }

  const undefinedBipf = bipf.allocAndEncode(undefined)

  function checkEqual(opData, buffer, pValue) {
    const fieldStart = opData.seek(buffer, 0, pValue)

    if (fieldStart === -1 && opData.value.equals(undefinedBipf)) return true
    else return bipf.compare(buffer, fieldStart, opData.value, 0) === 0
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
    const pValue = bipf.seekKey2(buffer, 0, BIPF_VALUE, 0)

    if (op.data.indexName === 'timestamp') {
      const timestamp = seekMinTimestamp(buffer, pValue)
      return compareWithRangeOp(op, timestamp)
    } else if (op.data.indexName === 'sequence') {
      const sequence = seekSequence(buffer, pValue)
      return compareWithRangeOp(op, sequence)
    } else {
      console.warn(
        `Attempted to do a ${op.type} comparison on unsupported index ${op.data.indexName}`
      )
      return true
    }
  }

  function checkPredicate(opData, buffer, pValue) {
    const fieldStart = opData.seek(buffer, 0, pValue)
    const predicateFn = opData.value
    if (fieldStart < 0) return false
    const fieldValue = bipf.decode(buffer, fieldStart)
    return predicateFn(fieldValue)
  }

  function checkAbsent(opData, buffer, pValue) {
    const fieldStart = opData.seek(buffer, 0, pValue)
    return fieldStart < 0
  }

  function checkIncludes(opData, buffer, pValue) {
    const fieldStart = opData.seek(buffer, 0, pValue)
    if (!~fieldStart) return false
    const type = bipf.getEncodedType(buffer, fieldStart)

    if (type === bipf.types.array) {
      let found = false
      bipf.iterate(buffer, fieldStart, (_, itemStart) => {
        const valueStart = opData.pluck
          ? opData.pluck(buffer, itemStart)
          : itemStart
        if (bipf.compare(buffer, valueStart, opData.value, 0) === 0) {
          found = true
          return true // abort the bipf.iterate
        }
      })
      return found
    } else return checkEqual(opData, buffer, pValue)
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

  function updatePrefixMapIndex(opData, index, buffer, seq, offset, pValue) {
    if (seq > index.count - 1) {
      const fieldStart = opData.seek(buffer, 0, pValue)
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

  function updatePrefixIndex(opData, index, buffer, seq, offset, pValue) {
    if (seq > index.count - 1) {
      if (seq > index.tarr.length - 1) growTarrIndex(index, Uint32Array)

      const fieldStart = opData.seek(buffer, 0, pValue)
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

  function updateIndexValue(op, index, buffer, seq, pValue) {
    if (op.type === 'EQUAL' && checkEqual(op.data, buffer, pValue))
      index.bitset.add(seq)
    else if (op.type === 'PREDICATE' && checkPredicate(op.data, buffer, pValue))
      index.bitset.add(seq)
    else if (op.type === 'ABSENT' && checkAbsent(op.data, buffer, pValue))
      index.bitset.add(seq)
    else if (op.type === 'INCLUDES' && checkIncludes(op.data, buffer, pValue))
      index.bitset.add(seq)
  }

  function updateAllIndexValue(opData, newIndexes, buffer, seq, pValue) {
    const fieldStart = opData.seek(buffer, 0, pValue)
    const value = bipf.decode(buffer, fieldStart)
    const indexName = safeFilename(opData.indexType + '_' + value)

    if (!newIndexes.has(indexName)) {
      newIndexes.set(indexName, {
        offset: 0,
        bitset: new TypedFastBitSet(),
        version: opData.version || 1,
      })
    }

    newIndexes.get(indexName).bitset.add(seq)
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

  const updateIndexesLock = mutexify()

  function updateIndexes(ops, cb) {
    updateIndexesLock(function onUpdateIndexesLockReleased(unlock) {
      const oldOps = ops
        .filter((op) => indexes.has(op.data.indexName))
        .map((op) => {
          if (coreIndexNames.includes(op.data.indexName)) op.isCore = true
          return op
        })
      const newOps = ops.filter((op) => !indexes.has(op.data.indexName))
      const oldIndexNames = oldOps.map((op) => op.data.indexName)
      const newIndexNames = newOps.map((op) => op.data.indexName)

      const indexNamesForStatus = [...coreIndexNames, ...oldIndexNames]

      // Reset old index if version was bumped
      for (const op of oldOps) {
        const index = indexes.get(op.data.indexName)
        if (op.data.version > index.version) {
          index.offset = -1
          index.count = 0
        }
      }

      // Prepare new indexes
      const newIndexes = new Map()
      for (const op of newOps) {
        if (op.data.prefix && op.data.useMap)
          newIndexes.set(op.data.indexName, {
            offset: 0,
            count: 0,
            map: {},
            prefix: typeof op.data.prefix === 'number' ? op.data.prefix : 32,
            version: op.data.version || 1,
          })
        else if (op.data.prefix)
          newIndexes.set(op.data.indexName, {
            offset: 0,
            count: 0,
            tarr: new Uint32Array(16 * 1000),
            prefix: typeof op.data.prefix === 'number' ? op.data.prefix : 32,
            version: op.data.version || 1,
          })
        else
          newIndexes.set(op.data.indexName, {
            offset: 0,
            bitset: new TypedFastBitSet(),
            version: op.data.version || 1,
          })
      }

      const latestOffset =
        newOps.length > 0
          ? -1
          : Math.min(...oldIndexNames.map((name) => indexes.get(name).offset))

      if (latestOffset === log.since.value && latestOffset >= 0) {
        unlock(cb)
        return
      }

      let seq = getSeqFromOffset(latestOffset) + 1

      let updatedSeqIndex = false
      let updatedTimestampIndex = false
      let updatedSequenceIndex = false
      const startSeq = seq
      const start = Date.now()
      let lastSaved = start

      function save(count, offset, doneIndexing) {
        const done = multicb({ pluck: 1 })
        if (updatedSeqIndex) saveCoreIndex('seq', seqIndex, count, done())

        if (updatedTimestampIndex)
          saveCoreIndex('timestamp', timestampIndex, count, done())

        if (updatedSequenceIndex)
          saveCoreIndex('sequence', sequenceIndex, count, done())

        for (const op of oldOps) {
          const indexName = op.data.indexName
          const index = indexes.get(indexName)
          if (index.offset < offset) {
            index.offset = offset
            if (op.data.version > index.version) index.version = op.data.version
          }
        }
        for (const [indexName, index] of newIndexes) {
          index.offset = offset
          if (doneIndexing) indexes.set(indexName, index)
        }

        done(() => {
          for (const op of oldOps) {
            if (op.isCore) continue
            const indexName = op.data.indexName
            const index = indexes.get(indexName)
            saveIndex(indexName, index, count)
          }
          for (const [indexName, index] of newIndexes) {
            saveIndex(indexName, index, count)
          }
        })
      }

      const logstreamId = Math.ceil(Math.random() * 1000)
      // prettier-ignore
      debug(`log.stream #${logstreamId} started, updating indexes ${oldIndexNames.concat(newIndexNames).join('|')} from offset ${latestOffset}`)
      status.update(indexes, indexNamesForStatus)
      status.update(newIndexes, newIndexNames)
      indexingActive.set(indexingActive.value + 1)

      log.stream({ gt: latestOffset }).pipe({
        paused: false,
        write(record) {
          const offset = record.offset
          const buffer = record.value

          if (updateSeqIndex(seq, offset)) updatedSeqIndex = true

          if (!buffer) {
            // deleted
            seq++
            return
          }

          const pValue = bipf.seekKey2(buffer, 0, BIPF_VALUE, 0)

          if (updateTimestampIndex(seq, offset, buffer, pValue))
            updatedTimestampIndex = true

          if (updateSequenceIndex(seq, offset, buffer, pValue))
            updatedSequenceIndex = true

          for (const op of oldOps) {
            if (op.isCore) continue
            const index = indexes.get(op.data.indexName)
            if (op.data.prefix && op.data.useMap)
              updatePrefixMapIndex(op.data, index, buffer, seq, offset, pValue)
            else if (op.data.prefix)
              updatePrefixIndex(op.data, index, buffer, seq, offset, pValue)
            else updateIndexValue(op, index, buffer, seq, pValue)
          }

          for (const op of newOps) {
            const index = newIndexes.get(op.data.indexName)
            if (op.data.prefix && op.data.useMap)
              updatePrefixMapIndex(op.data, index, buffer, seq, offset, pValue)
            else if (op.data.prefix)
              updatePrefixIndex(op.data, index, buffer, seq, offset, pValue)
            else if (op.data.indexAll)
              updateAllIndexValue(op.data, newIndexes, buffer, seq, pValue)
            else updateIndexValue(op, index, buffer, seq, pValue)
          }

          if (seq % 1000 === 0) {
            status.update(indexes, indexNamesForStatus)
            status.update(newIndexes, newIndexNames)
            const now = Date.now()
            if (now - lastSaved >= 60e3) {
              lastSaved = now
              save(seq + 1, offset, false)
            }
          }

          seq++
        },
        end() {
          // prettier-ignore
          debug(`log.stream #${logstreamId} ended, scanned ${seq - startSeq} records in ${Date.now() - start}ms`)

          const count = seq // incremented at the end of write()
          save(count, seqIndex.offset, true)

          status.update(indexes, indexNamesForStatus)
          status.update(newIndexes, newIndexNames)
          status.done(indexNamesForStatus)
          status.done(newIndexNames)
          indexingActive.set(indexingActive.value - 1)

          unlock(cb)
        },
      })
    })
  }

  // concurrent index load
  const waitingIndexLoad = new Map()

  function loadLazyIndex(indexName, cb) {
    if (onlyOneIndexAtATime(waitingIndexLoad, indexName, cb)) return

    debug('lazy loading %s', indexName)
    let index = indexes.get(indexName)
    if (index.prefix && index.map) {
      loadPrefixMapFile(index.filepath, (err, data) => {
        if (err) {
          debug('index %s failed to load with %s', indexName, err)
          indexes.delete(indexName)
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
          indexes.delete(indexName)
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
          indexes.delete(indexName)
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
    const index = indexes.get(op.data.indexName)
    if (log.since.value > index.offset || op.data.version > index.version) {
      updateIndexes([op], cb)
    } else {
      cb()
    }
  }

  function filterIndex(op, filterCheck, cb) {
    if (op.data.indexName === 'sequence') {
      const bitset = new TypedFastBitSet()
      const { tarr, count } = sequenceIndex
      for (let seq = 0; seq < count; ++seq) {
        if (filterCheck(tarr[seq], op)) bitset.add(seq)
      }
      cb(bitset)
    } else if (op.data.indexName === 'timestamp') {
      const bitset = new TypedFastBitSet()
      const { tarr, count } = timestampIndex
      for (let seq = 0; seq < count; ++seq) {
        if (filterCheck(tarr[seq], op)) bitset.add(seq)
      }
      cb(bitset)
    } else {
      debug('filterIndex() is unsupported for %s', op.data.indexName)
    }
  }

  function getFullBitset(cb) {
    const bitset = new TypedFastBitSet()
    const { count } = sequenceIndex
    bitset.addRange(0, count)
    cb(bitset)
  }

  function getOffsetsBitset(opOffsets, cb) {
    const seqs = []
    opOffsets.sort((x, y) => x - y)
    const opOffsetsLen = opOffsets.length
    const { tarr, count } = seqIndex
    for (let seq = 0; seq < count; ++seq) {
      if (bsb.eq(opOffsets, tarr[seq]) !== -1) seqs.push(seq)
      if (seqs.length === opOffsetsLen) break
    }
    cb(new TypedFastBitSet(seqs))
  }

  function matchAgainstPrefix(op, prefixIndex, cb) {
    const target = op.data.value
    const targetPrefix = target
      ? safeReadUint32(bipf.slice(target, 0), op.data.prefixOffset)
      : 0
    const bitset = new TypedFastBitSet()
    const bitsetFilters = new Map()

    const seek = op.data.seek
    function checker(value) {
      if (!value) return false // deleted

      const pValue = bipf.seekKey2(value, 0, BIPF_VALUE, 0)
      const fieldStart = seek(value, 0, pValue)

      if (target) return bipf.compare(value, fieldStart, target, 0) === 0
      else if (~fieldStart) return false

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
    } else if (op.type === 'ABSENT') {
      return `ABSENT(${op.data.indexType})`
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
      op.type === 'PREDICATE' ||
      op.type === 'ABSENT'
    ) {
      if (op.data.prefix) {
        matchAgainstPrefix(op, indexes.get(op.data.indexName), cb)
      } else {
        cb(indexes.get(op.data.indexName).bitset)
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
      getOffsetsBitset(op.offsets, cb)
    } else if (op.type === 'SEQS') {
      cb(new TypedFastBitSet(op.seqs))
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
    } else if (op.type === 'DEFERRED') {
      // DEFERRED only appears in this pipeline when using `prepare()` API
      op.task(
        { jitdb },
        (err, newOp) => {
          if (err) {
            console.error('Error executing DEFERRED operation', err)
            cb(null, new TypedFastBitSet())
            return
          }
          if (newOp) {
            getBitsetForOperation(newOp, cb)
          } else {
            cb(new TypedFastBitSet())
          }
        },
        function onAbort() {}
      )
    } else console.error('Unknown type in jitdb executeOperation:', op)
  }

  function forEachLeafOperationIn(operation, fn) {
    function forEachInto(ops) {
      ops.forEach((op) => {
        if (
          op.type === 'EQUAL' ||
          op.type === 'INCLUDES' ||
          op.type === 'PREDICATE' ||
          op.type === 'ABSENT'
        ) {
          fn(op)
        } else if (op.type === 'AND' || op.type === 'OR' || op.type === 'NOT')
          forEachInto(op.data)
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
    forEachInto([operation])
  }

  function detectLazyIndexesUsed(operation) {
    const results = []
    forEachLeafOperationIn(operation, (op) => {
      const name = op.data.indexName
      if (!indexes.has(name)) return
      if (indexes.get(name).lazy) results.push(name)
    })
    return results
  }

  function executeOperation(operation, cb) {
    updateCacheWithLog()
    if (bitsetCache.has(operation)) return cb(null, bitsetCache.get(operation))

    push(
      // kick-start this chain with a dummy null value
      push.values([null]),

      // load lazy indexes, if any
      push.asyncMap((_, next) => {
        const lazyIdxNames = detectLazyIndexesUsed(operation)
        if (lazyIdxNames.length === 0) return next()
        push(
          push.values(lazyIdxNames),
          push.asyncMap(loadLazyIndex),
          push.drain(null, next)
        )
      }),

      // update all relevant indexes, creating missing indexes, if any
      //
      // this needs to happen after loading lazy indexes because some
      // lazy indexes may have failed to load, and are now considered missing
      push.asyncMap((_, next) => {
        const ops = []
        ops.push({ data: { indexName: 'seq' } }) // always ensure seq updated
        forEachLeafOperationIn(operation, (op) => {
          ops.push(op)
        })
        updateIndexes(ops, next)
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
    if (!value) return false
    const pValue = bipf.seekKey2(value, 0, BIPF_VALUE, 0)

    for (let i = 0; i < ops.length; ++i) {
      const op = ops[i]
      let ok = false
      if (op.type === 'EQUAL') ok = checkEqual(op.data, value, pValue)
      else if (op.type === 'PREDICATE')
        ok = checkPredicate(op.data, value, pValue)
      else if (op.type === 'ABSENT') ok = checkAbsent(op.data, value, pValue)
      else if (op.type === 'INCLUDES')
        ok = checkIncludes(op.data, value, pValue)
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
    const offset = seqIndex.tarr[seq]
    log.get(offset, (err, recBuffer) => {
      if (err && err.code === 'ERR_AAOL_DELETED_RECORD') cb()
      else if (err) cb(err)
      else cb(null, bipf.decode(recBuffer, 0))
    })
  }

  function getRecord(seq, cb) {
    const offset = seqIndex.tarr[seq]
    log.get(offset, (err, value) => {
      if (err && err.code === 'ERR_AAOL_DELETED_RECORD')
        cb(null, { seq, offset })
      else if (err) cb(err)
      else cb(null, { offset, value, seq })
    })
  }

  function compareTSAscending(a, b) {
    return b.timestamp > a.timestamp
  }

  function compareTSDescending(a, b) {
    return a.timestamp > b.timestamp
  }

  function compareSeqAscending(a, b) {
    return b.seq > a.seq
  }

  function compareSeqDescending(a, b) {
    return a.seq > b.seq
  }

  function sortedBy(bitset, descending, sortBy) {
    updateCacheWithLog()
    const order = descending ? 'descending' : 'ascending'
    const sortedCache = sortBy === 'arrival' ? sortedSeqCache : sortedTSCache
    const comparer =
      sortBy === 'arrival'
        ? descending
          ? compareSeqDescending
          : compareSeqAscending
        : descending
        ? compareTSDescending
        : compareTSAscending

    if (sortedCache[order].has(bitset)) return sortedCache[order].get(bitset)
    const fpq = new FastPriorityQueue(comparer)
    fpq.heapify(
      bitset.array().map((seq) => {
        return {
          seq,
          timestamp: timestampIndex.tarr[seq],
        }
      })
    )
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
    sortBy,
    cb
  ) {
    seq = seq || 0

    const sorted = sortedBy(bitset, descending, sortBy)
    const total = sorted.size

    // seq -> record buffer
    const recBufferCache = {}

    function processResults(seqs) {
      push(
        push.values(seqs),
        push.asyncMap((seq, continueCB) => {
          if (onlyOffset) continueCB(null, seqIndex.tarr[seq])
          else getMessage(seq, recBufferCache, continueCB)
        }),
        push.filter((x) => (onlyOffset ? true : x)), // removes deleted messages
        push.collect((err, results) => {
          const more = seqs.length || limit
          cb(err, { results, total, nextSeq: seq + more })
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
        else processResults(seqs)
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
      processResults(slicedSeqs)
    }
  }

  // Scanning the log for this purpose is much slower than the keys index in
  // ssb-db2, but we don't need this to be fast because it's a one-time job only
  // after compaction has ended. We should use ssb-db2 `keys` in the future.
  function findSeqForMsgKey(msgKey, cb) {
    let seq = 0
    log.stream({ offsets: false, values: true, decrypt: false }).pipe(
      push.drain(
        function sinkToFindSeq(buffer) {
          if (!buffer) {
            seq += 1
            return
          }
          const pKey = bipf.seekKey2(buffer, 0, BIPF_KEY, 0)
          const msgKeyCandidate = bipf.decode(buffer, pKey)
          if (msgKeyCandidate === msgKey) {
            cb(null, seq)
            return false // abort sink
          } else {
            seq += 1
          }
        },
        function sinkEndedLookingForSeq() {
          cb(new Error('msgKey not found'))
        }
      )
    )
  }

  function countBitsetSlice(bitset, seq, descending) {
    if (!seq) return bitset.size()
    else return bitset.size() - seq
  }

  function paginate(
    operation,
    seq,
    limit,
    descending,
    onlyOffset,
    sortBy,
    latestMsgKeyPrecompaction,
    cb
  ) {
    if (!log.compactionProgress.value.done) {
      waitingCompaction.add(() => {
        if (latestMsgKeyPrecompaction) {
          findSeqForMsgKey(latestMsgKeyPrecompaction, (err, seqForMsgKey) => {
            if (err) return cb(err)
            const resumeSeq = seqForMsgKey + 1
            // prettier-ignore
            paginate(operation, resumeSeq, limit, descending, onlyOffset, sortBy, null, cb)
          })
        } else {
          // prettier-ignore
          paginate(operation, seq, limit, descending, onlyOffset, sortBy, null, cb)
        }
      })
      return
    }

    queriesActive.set(queriesActive.value + 1)
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
          sortBy,
          (err1, answer) => {
            queriesActive.set(queriesActive.value - 1)
            if (err1) return cb(err1)
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
        )
      })
    })
  }

  function all(operation, seq, descending, onlyOffset, sortBy, cb) {
    if (!log.compactionProgress.value.done) {
      waitingCompaction.add(() =>
        all(operation, seq, descending, onlyOffset, sortBy, cb)
      )
      return
    }

    queriesActive.set(queriesActive.value + 1)
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
          sortBy,
          (err1, answer) => {
            queriesActive.set(queriesActive.value - 1)
            if (err1) return cb(err1)
            answer.duration = Date.now() - start
            if (debugQuery.enabled)
              debugQuery(
                `all(${getNameFromOperation(operation)}): ${
                  answer.duration
                }ms, total messages: ${answer.total}`.replace(/%/g, '%% ')
              )
            cb(null, answer.results)
          }
        )
      })
    })
  }

  function count(operation, seq, descending, cb) {
    if (!log.compactionProgress.value.done) {
      waitingCompaction.add(() => count(operation, seq, descending, cb))
      return
    }

    queriesActive.set(queriesActive.value + 1)
    onReady(() => {
      const start = Date.now()
      executeOperation(operation, (err0, result) => {
        queriesActive.set(queriesActive.value - 1)
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

  function prepare(operation, cb) {
    if (!log.compactionProgress.value.done) {
      waitingCompaction.add(() => prepare(operation, cb))
      return
    }

    queriesActive.set(queriesActive.value + 1)
    onReady(() => {
      const start = Date.now()
      // Update status at the beginning:
      const indexNamesToReportStatus = []
      const indexesToReportStatus = new Map()
      forEachLeafOperationIn(operation, (op) => {
        const name = op.data.indexName
        if (!indexes.has(name)) {
          indexNamesToReportStatus.push(name)
          indexesToReportStatus.set(name, { offset: -1 })
        }
      })
      status.update(indexesToReportStatus, indexNamesToReportStatus)
      // Build indexes:
      executeOperation(operation, (err) => {
        queriesActive.set(queriesActive.value - 1)
        if (err) return cb(err)
        const duration = Date.now() - start
        cb(null, duration)
      })
    })
  }

  function lookup(opOrIndexName, seq, cb) {
    const op =
      typeof opOrIndexName === 'string'
        ? { data: { indexName: opOrIndexName } }
        : opOrIndexName
    onReady(() => {
      const indexName = op.data.indexName
      if (!indexes.has(indexName)) {
        return cb(new Error(`Cannot lookup, index ${indexName} not found`))
      }
      if (indexes.get(indexName).lazy) {
        return cb(new Error(`Cannot lookup, index ${indexName} not loaded`))
      }
      ensureIndexSync(op, () => {
        const result = indexes.get(indexName).tarr[seq]
        cb(null, result)
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
              op.type === 'PREDICATE' ||
              op.type === 'ABSENT'
            ) {
              if (!indexes.has(op.data.indexName)) offset = -1
              else offset = indexes.get(op.data.indexName).offset
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
              ensureIndexSync({ data: { indexName: 'seq' } }, () => {
                getRecord(seq, cb)
              })
            })
          )
        } else {
          const opts =
            offset === -1
              ? { live: true, gt: seqIndex.offset }
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
      seq = getSeqFromOffset(offset)
      prevOffset = seqIndex.tarr[seq - 1]
    }

    if (prevOffset === 0 && seq === seqIndex.count) {
      // not found
      seq = 1
    }

    function resetIndex(index) {
      if (index.offset >= prevOffset) {
        if (index.count) index.count = seq

        if (index.map) {
          // prefix map pushes to arrays, so we need to clean up
          for (let [prefix, arr] of Object.entries(index.map)) {
            if (seq === 0) index.map[prefix].length = 0
            else index.map[prefix] = arr.filter((x) => x < seq)
          }
        }

        if (index.bitset) {
          if (seq === 0) index.bitset.clear()
          else index.bitset.removeRange(seq, Infinity)
        }

        index.offset = prevOffset
      }
    }

    push(
      push.values([...indexes.entries()]),
      push.asyncMap(([indexName, index], cb) => {
        if (coreIndexNames.includes(indexName)) {
          resetIndex(index)
          saveCoreIndex(indexName, index, seq)
          cb()
        } else if (index.lazy) {
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
      push.drain(null, (err) => {
        if (err) return cb(err)

        clearCache()
        cb()
      })
    )
  }

  jitdb = {
    onReady,
    paginate,
    all,
    count,
    prepare,
    lookup,
    live,
    status: status.obv,
    reindex,
    indexingActive,
    queriesActive,

    // testing
    indexes,
  }
  return jitdb
}
