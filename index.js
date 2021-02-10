const path = require('path')
const bipf = require('bipf')
const push = require('push-stream')
const pull = require('pull-stream')
const toPull = require('push-stream-to-pull-stream')
const pullAsync = require('pull-async')
const TypedFastBitSet = require('typedfastbitset')
const bsb = require('binary-search-bounds')
const multicb = require('multicb')
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

  loadIndexes(() => {
    debug('loaded indexes', Object.keys(indexes))

    if (!indexes['seq']) {
      indexes['seq'] = {
        offset: -1,
        count: 0,
        tarr: new Uint32Array(16 * 1000),
      }
    }
    if (!indexes['timestamp']) {
      indexes['timestamp'] = {
        offset: -1,
        count: 0,
        tarr: new Float64Array(16 * 1000),
      }
    }
    if (!indexes['sequence']) {
      indexes['sequence'] = {
        offset: -1,
        count: 0,
        tarr: new Uint32Array(16 * 1000),
      }
    }

    status.batchUpdate(indexes, ['seq', 'timestamp', 'sequence'])

    isReady = true
    for (let i = 0; i < waiting.length; ++i) waiting[i]()
    waiting = []
  })

  function onReady(cb) {
    if (isReady) cb()
    else waiting.push(cb)
  }

  const bTimestamp = Buffer.from('timestamp')
  const bSequence = Buffer.from('sequence')
  const bValue = Buffer.from('value')

  // FIXME: handle the errors in these callbacks
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
              (e, idx) => {
                indexes[indexName] = idx
                cb()
              }
            )
          } else if (file === 'timestamp.index') {
            loadTypedArrayFile(
              path.join(indexesPath, file),
              Float64Array,
              (e, idx) => {
                indexes[indexName] = idx
                cb()
              }
            )
          } else if (file === 'sequence.index') {
            loadTypedArrayFile(
              path.join(indexesPath, file),
              Uint32Array,
              (e, idx) => {
                indexes[indexName] = idx
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
      coreIndex.version || 1,
      coreIndex.offset,
      count,
      coreIndex.tarr
    )
  }

  function saveIndex(name, index, cb) {
    if (index.offset < 0 || index.bitset.size() === 0) return
    debug('saving index: %s', name)
    const filename = path.join(indexesPath, name + '.index')
    saveBitsetFile(filename, index.version || 1, index.offset, index.bitset, cb)
  }

  function savePrefixIndex(name, prefixIndex, count, cb) {
    if (prefixIndex.offset < 0) return
    debug('saving prefix index: %s', name)
    const num = prefixIndex.prefix
    const filename = path.join(indexesPath, name + `.${num}prefix`)
    saveTypedArrayFile(
      filename,
      prefixIndex.version || 1,
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
      prefixIndex.version || 1,
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

  function updateTimestampIndex(seq, offset, buffer) {
    if (seq > indexes['timestamp'].count - 1) {
      if (seq > indexes['timestamp'].tarr.length - 1)
        growTarrIndex(indexes['timestamp'], Float64Array)

      indexes['timestamp'].offset = offset

      var p = 0 // note you pass in p!
      p = bipf.seekKey(buffer, p, bTimestamp)
      const arrivalTimestamp = bipf.decode(buffer, p)
      p = 0
      p = bipf.seekKey(buffer, p, bValue)
      p = bipf.seekKey(buffer, p, bTimestamp)
      const declaredTimestamp = bipf.decode(buffer, p)
      const timestamp = Math.min(arrivalTimestamp, declaredTimestamp)

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

      var p = 0 // note you pass in p!
      p = bipf.seekKey(buffer, p, bValue)
      p = bipf.seekKey(buffer, p, bSequence)

      indexes['sequence'].tarr[seq] = bipf.decode(buffer, p)
      indexes['sequence'].count = seq + 1
      return true
    }
  }

  function checkEqual(opData, buffer) {
    const fieldStart = opData.seek(buffer)
    if (!opData.value) return fieldStart === -1
    else if (
      ~fieldStart &&
      bipf.compareString(buffer, fieldStart, opData.value) === 0
    )
      return true
    else return false
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

    const indexNamesForStatus = [
      'seq',
      'timestamp',
      'sequence',
      op.data.indexName,
    ]

    const waitingKey = op.data.indexName
    if (onlyOneIndexAtATime(waitingIndexUpdate, waitingKey, cb)) return

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
    const start = Date.now()

    const indexNeedsUpdate =
      op.data.indexName !== 'sequence' &&
      op.data.indexName !== 'timestamp' &&
      op.data.indexName !== 'seq'

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
        }

        seq++
      },
      end: () => {
        const count = seq // incremented at end
        debug(`time: ${Date.now() - start}ms, total items: ${count}`)

        if (updatedSeqIndex) saveCoreIndex('seq', indexes['seq'], count)

        if (updatedTimestampIndex)
          saveCoreIndex('timestamp', indexes['timestamp'], count)

        if (updatedSequenceIndex)
          saveCoreIndex('sequence', indexes['sequence'], count)

        index.offset = indexes['seq'].offset
        if (indexNeedsUpdate) {
          if (index.prefix && index.map)
            savePrefixMapIndex(op.data.indexName, index, count)
          else if (index.prefix)
            savePrefixIndex(op.data.indexName, index, count)
          else saveIndex(op.data.indexName, index)
        }

        status.batchUpdate(indexes, indexNamesForStatus)

        runWaitingIndexLoadCbs(waitingIndexUpdate, waitingKey)

        cb()
      },
    })
  }

  // concurrent index create
  const waitingIndexCreate = new Map()

  function createIndexes(opsMissingIndexes, cb) {
    const newIndexes = {}

    const coreIndexNames = ['seq', 'timestamp', 'sequence']
    const newIndexNames = opsMissingIndexes.map((op) => op.data.indexName)

    const waitingKey = newIndexNames.join('|')
    if (onlyOneIndexAtATime(waitingIndexCreate, waitingKey, cb)) return

    opsMissingIndexes.forEach((op) => {
      if (op.data.prefix && op.data.useMap) {
        newIndexes[op.data.indexName] = {
          offset: 0,
          count: 0,
          map: {},
          prefix: typeof op.data.prefix === 'number' ? op.data.prefix : 32,
        }
      } else if (op.data.prefix)
        newIndexes[op.data.indexName] = {
          offset: 0,
          count: 0,
          tarr: new Uint32Array(16 * 1000),
          prefix: typeof op.data.prefix === 'number' ? op.data.prefix : 32,
        }
      else
        newIndexes[op.data.indexName] = {
          offset: 0,
          bitset: new TypedFastBitSet(),
        }
    })

    let seq = 0

    let updatedSeqIndex = false
    let updatedTimestampIndex = false
    let updatedSequenceIndex = false
    const start = Date.now()

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

        opsMissingIndexes.forEach((op) => {
          if (op.data.prefix && op.data.useMap)
            updatePrefixMapIndex(
              op.data,
              newIndexes[op.data.indexName],
              buffer,
              seq,
              offset
            )
          if (op.data.prefix)
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
        }

        seq++
      },
      end: () => {
        const count = seq // incremented at end
        debug(`time: ${Date.now() - start}ms, total items: ${count}`)

        if (updatedSeqIndex) saveCoreIndex('seq', indexes['seq'], count)

        if (updatedTimestampIndex)
          saveCoreIndex('timestamp', indexes['timestamp'], count)

        if (updatedSequenceIndex)
          saveCoreIndex('sequence', indexes['sequence'], count)

        for (var indexName in newIndexes) {
          const index = (indexes[indexName] = newIndexes[indexName])
          index.offset = indexes['seq'].offset
          if (index.prefix && index.map)
            savePrefixMapIndex(indexName, index, count)
          else if (index.prefix) savePrefixIndex(indexName, index, count)
          else saveIndex(indexName, index)
        }

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
        if (err) return cb(err)
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
        if (err) return cb(err)
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
        if (err) return cb(err)
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

  function loadLazyIndexes(indexNames, cb) {
    push(
      push.values(indexNames),
      push.asyncMap(loadLazyIndex),
      push.collect(cb)
    )
  }

  function ensureIndexSync(op, cb) {
    if (log.since.value > indexes[op.data.indexName].offset) {
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
    const done = multicb({ pluck: 1 })

    if (prefixIndex.map) {
      if (prefixIndex.map[targetPrefix]) {
        prefixIndex.map[targetPrefix].forEach((seq) => {
          bitset.add(seq)
          getRecord(seq, done())
        })
      }
    } else {
      const count = prefixIndex.count
      const tarr = prefixIndex.tarr
      for (let seq = 0; seq < count; ++seq) {
        if (tarr[seq] === targetPrefix) {
          bitset.add(seq)
          getRecord(seq, done())
        }
      }
    }

    done((err, recs) => {
      // FIXME: handle error better, this cb() should support 2 args
      if (err) return console.error(err)
      const seek = op.data.seek
      for (let i = 0, len = recs.length; i < len; ++i) {
        const { value, seq } = recs[i]
        if (!value) {
          // deleted
          bitset.remove(seq)
          continue
        }
        const fieldStart = seek(value)
        const candidate = bipf.slice(value, fieldStart)
        if (target) {
          if (Buffer.compare(candidate, target)) bitset.remove(seq)
        } else {
          if (~fieldStart) bitset.remove(seq)
        }
      }
      cb(bitset)
    })
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
    if (op.type === 'EQUAL' || op.type === 'INCLUDES') {
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
    }
  }

  function getBitsetForOperation(op, cb) {
    if (op.type === 'EQUAL' || op.type === 'INCLUDES') {
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

      getBitsetForOperation(op.data[0], (op1) => {
        getBitsetForOperation(op.data[1], (op2) => {
          cb(op1.new_intersection(op2))
        })
      })
    } else if (op.type === 'OR') {
      if (op.data.length > 2) op = nestLargeOpsArray(op.data, 'OR')

      getBitsetForOperation(op.data[0], (op1) => {
        getBitsetForOperation(op.data[1], (op2) => {
          cb(op1.new_union(op2))
        })
      })
    } else if (op.type === 'NOT') {
      getBitsetForOperation(op.data[0], (op1) => {
        getFullBitset((fullBitset) => {
          cb(fullBitset.difference(op1))
        })
      })
    } else if (!op.type) {
      // to support `query(fromDB(jitdb), toCallback(cb))`
      getFullBitset(cb)
    } else console.error('Unknown type', op)
  }

  function executeOperation(operation, cb) {
    updateCacheWithLog()
    if (bitsetCache.has(operation)) return cb(bitsetCache.get(operation))

    const opsMissingIndexes = []
    const lazyIndexes = []

    function detectMissingAndLazyIndexes(ops) {
      ops.forEach((op) => {
        if (op.type === 'EQUAL' || op.type === 'INCLUDES') {
          const indexName = op.data.indexName
          if (!indexes[indexName]) opsMissingIndexes.push(op)
          else if (indexes[indexName].lazy) lazyIndexes.push(indexName)
        } else if (op.type === 'AND' || op.type === 'OR' || op.type === 'NOT')
          detectMissingAndLazyIndexes(op.data)
        else if (
          op.type === 'SEQS' ||
          op.type === 'LIVESEQS' ||
          op.type === 'OFFSETS' ||
          !op.type // e.g. query(fromDB, toCallback), or empty deferred()
        );
        else debug('Unknown operator type: ' + op.type)
      })
    }

    function getBitset() {
      getBitsetForOperation(operation, (bitset) => {
        bitsetCache.set(operation, bitset)
        cb(bitset)
      })
    }

    function createMissingIndexes() {
      if (opsMissingIndexes.length > 0)
        createIndexes(opsMissingIndexes, getBitset)
      else getBitset()
    }

    detectMissingAndLazyIndexes([operation])

    ensureSeqIndexSync(() => {
      if (opsMissingIndexes.length > 0)
        debug('missing indexes: %o', opsMissingIndexes)

      if (lazyIndexes.length > 0)
        loadLazyIndexes(lazyIndexes, createMissingIndexes)
      else createMissingIndexes()
    })
  }

  function isValueOk(ops, value, isOr) {
    for (let i = 0; i < ops.length; ++i) {
      const op = ops[i]
      let ok = false
      if (op.type === 'EQUAL') ok = checkEqual(op.data, value)
      else if (op.type === 'INCLUDES') ok = checkIncludes(op.data, value)
      else if (op.type === 'NOT') ok = !isValueOk(op.data, value, false)
      else if (op.type === 'AND') ok = isValueOk(op.data, value, false)
      else if (op.type === 'OR') ok = isValueOk(op.data, value, true)
      else if (op.type === 'LIVESEQS') ok = true
      else if (!op.type) ok = true

      if (ok && isOr) return true
      else if (!ok && !isOr) return false
    }

    if (isOr) return false
    else return true
  }

  function getMessage(seq, cb) {
    const offset = indexes['seq'].tarr[seq]
    log.get(offset, (err, value) => {
      if (err && err.code === 'flumelog:deleted') cb()
      else cb(err, bipf.decode(value, 0))
    })
  }

  function getRecord(seq, cb) {
    const offset = indexes['seq'].tarr[seq]
    log.get(offset, (err, value) => {
      if (err && err.code === 'flumelog:deleted') cb(null, { seq, offset })
      else cb(err, { offset, value, seq })
    })
  }

  function sortedByTimestamp(bitset, descending) {
    updateCacheWithLog()
    const order = descending ? 'descending' : 'ascending'
    if (sortedCache[order].has(bitset)) return sortedCache[order].get(bitset)
    const timestamped = bitset.array().map((seq) => {
      return {
        seq,
        timestamp: indexes['timestamp'].tarr[seq],
      }
    })
    const sorted = timestamped.sort((a, b) => {
      if (descending) return b.timestamp - a.timestamp
      else return a.timestamp - b.timestamp
    })
    sortedCache[order].set(bitset, sorted)
    return sorted
  }

  function getMessagesFromBitsetSlice(
    bitset,
    seq,
    limit,
    descending,
    onlyOffset,
    cb
  ) {
    seq = seq || 0

    const sorted = sortedByTimestamp(bitset, descending)
    const sliced =
      limit != null
        ? sorted.slice(seq, seq + limit)
        : seq > 0
        ? sorted.slice(seq)
        : sorted

    push(
      push.values(sliced),
      push.asyncMap(({ seq }, cb) => {
        if (onlyOffset) cb(null, indexes['seq'].tarr[seq])
        else getMessage(seq, cb)
      }),
      push.filter((x) => (onlyOffset ? true : x)), // removes deleted messages
      push.collect((err, results) => {
        cb(err, {
          results: results,
          total: sorted.length,
        })
      })
    )
  }

  function countBitsetSlice(bitset, seq, descending) {
    if (!seq) return bitset.size()
    else return sortedByTimestamp(bitset, descending).slice(seq).length
  }

  function paginate(operation, seq, limit, descending, onlyOffset, cb) {
    onReady(() => {
      const start = Date.now()
      executeOperation(operation, (bitset) => {
        getMessagesFromBitsetSlice(
          bitset,
          seq,
          limit,
          descending,
          onlyOffset,
          (err, answer) => {
            if (err) cb(err)
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
              cb(err, answer)
            }
          }
        )
      })
    })
  }

  function all(operation, seq, descending, onlyOffset, cb) {
    onReady(() => {
      const start = Date.now()
      executeOperation(operation, (bitset) => {
        getMessagesFromBitsetSlice(
          bitset,
          seq,
          null,
          descending,
          onlyOffset,
          (err, answer) => {
            if (err) cb(err)
            else {
              answer.duration = Date.now() - start
              if (debugQuery.enabled)
                debugQuery(
                  `all(${getNameFromOperation(operation)}): ${
                    answer.duration
                  }ms, total messages: ${answer.total}`.replace(/%/g, '%% ')
                )
              cb(err, answer.results)
            }
          }
        )
      })
    })
  }

  function count(operation, seq, descending, cb) {
    onReady(() => {
      const start = Date.now()
      executeOperation(operation, (bitset) => {
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
          executeOperation(op, (bitset) => {
            cb()
          })
        })
      ),
      pull.map(() => {
        let offset = -1
        let seqStream

        function detectOffsetAndSeqStream(ops) {
          ops.forEach((op) => {
            if (op.type === 'EQUAL' || op.type === 'INCLUDES') {
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

  return {
    onReady,
    paginate,
    all,
    count,
    live,
    status: status.obv,

    // testing
    indexes,
  }
}
