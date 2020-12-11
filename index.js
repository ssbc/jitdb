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
const {
  saveTypedArrayFile,
  loadTypedArrayFile,
  saveBitsetFile,
  loadBitsetFile,
  safeFilename,
  listFilesIDB,
  listFilesFS,
} = require('./files')

module.exports = function (log, indexesPath) {
  debug('indexes path', indexesPath)

  const indexes = {}
  let isReady = false
  let waiting = []

  loadIndexes(() => {
    debug('loaded indexes', Object.keys(indexes))

    if (!indexes['offset']) {
      indexes['offset'] = {
        seq: -1,
        count: 0,
        tarr: new Uint32Array(16 * 1000),
      }
    }
    if (!indexes['timestamp']) {
      indexes['timestamp'] = {
        seq: -1,
        count: 0,
        tarr: new Float64Array(16 * 1000),
      }
    }
    if (!indexes['sequence']) {
      indexes['sequence'] = {
        seq: -1,
        count: 0,
        tarr: new Uint32Array(16 * 1000),
      }
    }

    isReady = true
    for (var i = 0; i < waiting.length; ++i) waiting[i]()
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
          if (file === 'offset.index') {
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
              seq: -1,
              count: 0,
              tarr: new Uint32Array(16 * 1000),
              lazy: true,
              prefix: 32,
              filepath: path.join(indexesPath, file),
            }
            cb()
          } else if (file.endsWith('.index')) {
            // Don't load it yet, just tag it `lazy`
            indexes[indexName] = {
              seq: 0,
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

  function saveCoreIndex(name, coreIndex, count) {
    debug('saving core index: %s', name)
    const filename = path.join(indexesPath, name + '.index')
    saveTypedArrayFile(filename, coreIndex.seq, count, coreIndex.tarr)
  }

  function saveIndex(name, index, cb) {
    debug('saving index: %s', name)
    const filename = path.join(indexesPath, name + '.index')
    saveBitsetFile(filename, index.seq, index.bitset, cb)
  }

  function savePrefixIndex(name, prefixIndex, count, cb) {
    debug('saving prefix index: %s', name)
    const num = prefixIndex.prefix
    const filename = path.join(indexesPath, name + `.${num}prefix`)
    saveTypedArrayFile(filename, prefixIndex.seq, count, prefixIndex.tarr, cb)
  }

  function growTarrIndex(index, Type) {
    debug('growing index')
    const newArray = new Type(index.tarr.length * 2)
    newArray.set(index.tarr)
    index.tarr = newArray
  }

  function updateOffsetIndex(offset, seq) {
    if (offset > indexes['offset'].count - 1) {
      if (offset > indexes['offset'].tarr.length)
        growTarrIndex(indexes['offset'], Uint32Array)

      indexes['offset'].seq = seq
      indexes['offset'].tarr[offset] = seq
      indexes['offset'].count = offset + 1
      return true
    }
  }

  function updateTimestampIndex(offset, seq, buffer) {
    if (offset > indexes['timestamp'].count - 1) {
      if (offset > indexes['timestamp'].tarr.length)
        growTarrIndex(indexes['timestamp'], Float64Array)

      indexes['timestamp'].seq = seq

      var p = 0 // note you pass in p!
      p = bipf.seekKey(buffer, p, bValue)
      p = bipf.seekKey(buffer, p, bTimestamp)

      // FIXME: maybe min of the two timestamps:
      // https://github.com/ssbc/ssb-backlinks/blob/7a731d03acebcbb84b5fee5f0dcc4f6fef3b8035/emit-links.js#L55
      indexes['timestamp'].tarr[offset] = bipf.decode(buffer, p)
      indexes['timestamp'].count = offset + 1
      return true
    }
  }

  function updateSequenceIndex(offset, seq, buffer) {
    if (offset > indexes['sequence'].count - 1) {
      if (offset > indexes['sequence'].tarr.length)
        growTarrIndex(indexes['sequence'], Uint32Array)

      indexes['sequence'].seq = seq

      var p = 0 // note you pass in p!
      p = bipf.seekKey(buffer, p, bValue)
      p = bipf.seekKey(buffer, p, bSequence)

      indexes['sequence'].tarr[offset] = bipf.decode(buffer, p)
      indexes['sequence'].count = offset + 1
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

  function safeReadUint32(buf) {
    if (buf.length < 4) {
      const bigger = Buffer.alloc(4)
      buf.copy(bigger)
      return bigger.readUInt32LE(0)
    } else {
      return buf.readUInt32LE(0)
    }
  }

  function updatePrefixIndex(opData, index, buffer, offset, seq) {
    if (offset > index.count - 1) {
      if (offset > index.tarr.length) growTarrIndex(index, Uint32Array)

      const fieldStart = opData.seek(buffer)
      if (fieldStart) {
        const buf = bipf.slice(buffer, fieldStart)
        index.tarr[offset] = buf.length ? safeReadUint32(buf) : 0
      } else {
        index.tarr[offset] = 0
      }
      index.seq = seq
      index.count = offset + 1
    }
  }

  function updateIndexValue(op, index, buffer, offset) {
    if (op.type === 'EQUAL' && checkEqual(op.data, buffer))
      index.bitset.add(offset)
    else if (op.type === 'INCLUDES' && checkIncludes(op.data, buffer))
      index.bitset.add(offset)
  }

  function updateAllIndexValue(opData, newIndexes, buffer, offset) {
    const fieldStart = opData.seek(buffer)
    const value = bipf.decode(buffer, fieldStart)
    const indexName = safeFilename(opData.indexType + '_' + value)

    if (!newIndexes[indexName]) {
      newIndexes[indexName] = {
        seq: 0,
        bitset: new TypedFastBitSet(),
      }
    }

    newIndexes[indexName].bitset.add(offset)
  }

  function updateIndex(op, cb) {
    const index = indexes[op.data.indexName]

    // find the next possible offset
    let offset = 0
    if (index.seq !== -1) {
      const { tarr } = indexes['offset']
      const indexSeq = index.seq
      for (const len = tarr.length; offset < len; ++offset)
        if (tarr[offset] === indexSeq) {
          offset++
          break
        }
    }

    let updatedOffsetIndex = false
    let updatedTimestampIndex = false
    let updatedSequenceIndex = false
    const start = Date.now()

    const indexNeedsUpdate =
      op.data.indexName !== 'sequence' &&
      op.data.indexName !== 'timestamp' &&
      op.data.indexName !== 'offset'

    log.stream({ gt: index.seq }).pipe({
      paused: false,
      write: function (record) {
        if (updateOffsetIndex(offset, record.seq)) updatedOffsetIndex = true

        if (updateTimestampIndex(offset, record.seq, record.value))
          updatedTimestampIndex = true

        if (updateSequenceIndex(offset, record.seq, record.value))
          updatedSequenceIndex = true

        if (indexNeedsUpdate) {
          if (op.data.prefix)
            updatePrefixIndex(op.data, index, record.value, offset, record.seq)
          else updateIndexValue(op, index, record.value, offset)
        }

        offset++
      },
      end: () => {
        const count = offset // incremented at end
        debug(`time: ${Date.now() - start}ms, total items: ${count}`)

        if (updatedOffsetIndex)
          saveCoreIndex('offset', indexes['offset'], count)

        if (updatedTimestampIndex)
          saveCoreIndex('timestamp', indexes['timestamp'], count)

        if (updatedSequenceIndex)
          saveCoreIndex('sequence', indexes['sequence'], count)

        index.seq = indexes['offset'].seq
        if (indexNeedsUpdate) {
          if (index.prefix) savePrefixIndex(op.data.indexName, index, count)
          else saveIndex(op.data.indexName, index)
        }

        cb()
      },
    })
  }

  function createIndexes(opsMissingIndexes, cb) {
    const newIndexes = {}
    opsMissingIndexes.forEach((op) => {
      if (op.data.prefix)
        newIndexes[op.data.indexName] = {
          seq: 0,
          count: 0,
          tarr: new Uint32Array(16 * 1000),
          prefix: typeof op.data.prefix === 'number' ? op.data.prefix : 32,
        }
      else
        newIndexes[op.data.indexName] = {
          seq: 0,
          bitset: new TypedFastBitSet(),
        }
    })

    let offset = 0

    let updatedOffsetIndex = false
    let updatedTimestampIndex = false
    let updatedSequenceIndex = false
    const start = Date.now()

    log.stream({}).pipe({
      paused: false,
      write: function (record) {
        const seq = record.seq
        const buffer = record.value

        if (updateOffsetIndex(offset, seq)) updatedOffsetIndex = true

        if (updateTimestampIndex(offset, record.seq, buffer))
          updatedTimestampIndex = true

        if (updateSequenceIndex(offset, record.seq, buffer))
          updatedSequenceIndex = true

        opsMissingIndexes.forEach((op) => {
          if (op.data.prefix)
            updatePrefixIndex(
              op.data,
              newIndexes[op.data.indexName],
              buffer,
              offset,
              seq
            )
          else if (op.data.indexAll)
            updateAllIndexValue(op.data, newIndexes, buffer, offset)
          else
            updateIndexValue(op, newIndexes[op.data.indexName], buffer, offset)
        })

        offset++
      },
      end: () => {
        const count = offset // incremented at end
        debug(`time: ${Date.now() - start}ms, total items: ${count}`)

        if (updatedOffsetIndex)
          saveCoreIndex('offset', indexes['offset'], count)

        if (updatedTimestampIndex)
          saveCoreIndex('timestamp', indexes['timestamp'], count)

        if (updatedSequenceIndex)
          saveCoreIndex('sequence', indexes['sequence'], count)

        for (var indexName in newIndexes) {
          const index = (indexes[indexName] = newIndexes[indexName])
          index.seq = indexes['offset'].seq
          if (index.prefix) savePrefixIndex(indexName, index, count)
          else saveIndex(indexName, index)
        }

        cb()
      },
    })
  }

  function loadLazyIndex(indexName, cb) {
    debug('lazy loading %s', indexName)
    let index = indexes[indexName]
    if (index.prefix) {
      loadTypedArrayFile(
        index.filepath,
        Uint32Array,
        (err, { seq, tarr, count }) => {
          // FIXME: handle error
          index.seq = seq
          index.tarr = tarr
          index.count = count
          index.lazy = false
          cb()
        }
      )
    } else {
      loadBitsetFile(index.filepath, (err, { seq, bitset }) => {
        // FIXME: handle error
        index.seq = seq
        index.bitset = bitset
        index.lazy = false
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
    if (log.since.value > indexes[op.data.indexName].seq) {
      updateIndex(op, cb)
    } else {
      debug('ensureIndexSync %s is already synced', op.data.indexName)
      cb()
    }
  }

  function ensureOffsetIndexSync(cb) {
    ensureIndexSync({ data: { indexName: 'offset' } }, cb)
  }

  function filterIndex(op, filterCheck, cb) {
    ensureIndexSync(op, () => {
      if (op.data.indexName === 'sequence') {
        const bitset = new TypedFastBitSet()
        const { tarr, count } = indexes['sequence']
        for (var i = 0; i < count; ++i) {
          if (filterCheck(tarr[i], op)) bitset.add(i)
        }
        cb(bitset)
      } else if (op.data.indexName === 'timestamp') {
        const bitset = new TypedFastBitSet()
        const { tarr, count } = indexes['timestamp']
        for (var i = 0; i < count; ++i) {
          if (filterCheck(tarr[i], op)) bitset.add(i)
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
      for (var i = 0; i < count; ++i) bitset.add(i)
      cb(bitset)
    })
  }

  function getSeqsBitset(opSeqs, cb) {
    const offsets = []
    opSeqs.sort((x, y) => x - y)
    const opSeqsLen = opSeqs.length
    const { tarr } = indexes['offset']
    for (var o = 0, len = tarr.length; o < len; ++o) {
      if (bsb.eq(opSeqs, tarr[o]) !== -1) offsets.push(o)
      if (offsets.length === opSeqsLen) break
    }
    cb(new TypedFastBitSet(offsets))
  }

  function matchAgainstPrefix(op, prefixIndex, cb) {
    const target = op.data.value
    const targetPrefix = target ? safeReadUint32(target) : 0
    const count = prefixIndex.count
    const tarr = prefixIndex.tarr
    const bitset = new TypedFastBitSet()
    const done = multicb({ pluck: 1 })
    for (let o = 0; o < count; ++o) {
      if (tarr[o] === targetPrefix) {
        bitset.add(o)
        getRecord(o, done())
      }
    }
    done((err, recs) => {
      // FIXME: handle error better, this cb() should support 2 args
      if (err) return console.error(err)
      const seek = op.data.seek
      for (let i = 0, len = recs.length; i < len; ++i) {
        const { value, offset } = recs[i]
        const fieldStart = seek(value)
        const candidate = bipf.slice(value, fieldStart)
        if (target) {
          if (Buffer.compare(candidate, target)) bitset.remove(offset)
        } else {
          if (~fieldStart) bitset.remove(offset)
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
    } else if (op.type === 'SEQS') {
      ensureOffsetIndexSync(() => {
        getSeqsBitset(op.seqs, cb)
      })
    } else if (op.type === 'OFFSETS') {
      ensureOffsetIndexSync(() => {
        cb(new TypedFastBitSet(op.offsets))
      })
    } else if (op.type === 'LIVEOFFSETS') {
      ensureOffsetIndexSync(() => {
        cb(new TypedFastBitSet(op.offsets))
      })
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
    } else if (!op.type) {
      // to support `query(fromDB(jitdb), toCallback(cb))`
      getFullBitset(cb)
    } else console.error('Unknown type', op)
  }

  function executeOperation(operation, cb) {
    const opsMissingIndexes = []
    const lazyIndexes = []

    function detectMissingAndLazyIndexes(ops) {
      ops.forEach((op) => {
        if (op.type === 'EQUAL' || op.type === 'INCLUDES') {
          const indexName = op.data.indexName
          if (!indexes[indexName]) opsMissingIndexes.push(op)
          else if (indexes[indexName].lazy) lazyIndexes.push(indexName)
        } else if (op.type === 'AND' || op.type === 'OR')
          detectMissingAndLazyIndexes(op.data)
        else if (
          op.type === 'OFFSETS' ||
          op.type === 'LIVEOFFSETS' ||
          op.type === 'SEQS' ||
          !op.type // e.g. query(fromDB, toCallback), or empty deferred()
        );
        else debug('Unknown operator type: ' + op.type)
      })
    }

    function getBitset() {
      getBitsetForOperation(operation, cb)
    }

    function createMissingIndexes() {
      if (opsMissingIndexes.length > 0)
        createIndexes(opsMissingIndexes, getBitset)
      else getBitset()
    }

    detectMissingAndLazyIndexes([operation])

    if (opsMissingIndexes.length > 0)
      debug('missing indexes: %o', opsMissingIndexes)

    if (lazyIndexes.length > 0)
      loadLazyIndexes(lazyIndexes, createMissingIndexes)
    else createMissingIndexes()
  }

  function isValueOk(ops, value, isOr) {
    for (var i = 0; i < ops.length; ++i) {
      const op = ops[i]
      let ok = false
      if (op.type === 'EQUAL') ok = checkEqual(op.data, value)
      else if (op.type === 'INCLUDES') ok = checkIncludes(op.data, value)
      else if (op.type === 'AND') ok = isValueOk(op.data, value, false)
      else if (op.type === 'OR') ok = isValueOk(op.data, value, true)
      else if (op.type === 'LIVEOFFSETS') ok = true
      else if (!op.type) ok = true

      if (ok && isOr) return true
      else if (!ok && !isOr) return false
    }

    if (isOr) return false
    else return true
  }

  function getMessage(offset, cb) {
    const seq = indexes['offset'].tarr[offset]
    log.get(seq, (err, res) => {
      if (err && err.code === 'flumelog:deleted') cb()
      else cb(err, bipf.decode(res, 0))
    })
  }

  function getRecord(offset, cb) {
    const seq = indexes['offset'].tarr[offset]
    log.get(seq, (err, res) => {
      if (err && err.code === 'flumelog:deleted') cb()
      else cb(err, { seq, value: res, offset })
    })
  }

  function getMessagesFromBitsetSlice(bitset, offset, limit, descending, cb) {
    offset = offset || 0
    const start = Date.now()

    const timestamped = bitset.array().map((o) => {
      return {
        offset: o,
        timestamp: indexes['timestamp'].tarr[o],
      }
    })
    const sorted = timestamped.sort((a, b) => {
      if (descending) return b.timestamp - a.timestamp
      else return a.timestamp - b.timestamp
    })
    const sliced =
      limit != null
        ? sorted.slice(offset, offset + limit)
        : offset > 0
        ? sorted.slice(offset)
        : sorted

    push(
      push.values(sliced),
      push.asyncMap(({ offset }, cb) => getMessage(offset, cb)),
      push.filter((x) => x), // removes deleted messages
      push.collect((err, results) => {
        cb(err, {
          results: results,
          total: timestamped.length,
          duration: Date.now() - start,
        })
      })
    )
  }

  function paginate(operation, offset, limit, descending, cb) {
    onReady(() => {
      executeOperation(operation, (bitset) => {
        getMessagesFromBitsetSlice(
          bitset,
          offset,
          limit,
          descending,
          (err, answer) => {
            if (err) cb(err)
            else {
              debug(
                `paginate(): ${answer.duration}ms, total messages: ${answer.total}`
              )
              cb(err, answer)
            }
          }
        )
      })
    })
  }

  function all(operation, offset, descending, cb) {
    onReady(() => {
      executeOperation(operation, (bitset) => {
        getMessagesFromBitsetSlice(
          bitset,
          offset,
          null,
          descending,
          (err, answer) => {
            if (err) cb(err)
            else {
              debug(
                `all(): ${answer.duration}ms, total messages: ${answer.total}`
              )
              cb(err, answer.results)
            }
          }
        )
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
        let seq = -1
        let offsetStream

        function detectSeqAndOffsetStream(ops) {
          ops.forEach((op) => {
            if (op.type === 'EQUAL' || op.type === 'INCLUDES') {
              if (!indexes[op.data.indexName]) seq = -1
              else seq = indexes[op.data.indexName].seq
            } else if (op.type === 'AND' || op.type === 'OR') {
              detectSeqAndOffsetStream(op.data)
            } else if (op.type === 'LIVEOFFSETS') {
              if (offsetStream)
                throw new Error('Only one offset stream in live supported')
              offsetStream = op.stream
            }
          })
        }

        detectSeqAndOffsetStream([op])

        // There are two cases here:
        // - op contains a live offset stream, in which case we let the
        //   offset stream drive new values
        // - op doesn't, in which we let the log stream drive new values

        let recordStream
        if (offsetStream) {
          recordStream = pull(
            offsetStream,
            pull.asyncMap((o, cb) => {
              ensureOffsetIndexSync(() => {
                getRecord(o, cb)
              })
            })
          )
        } else {
          const opts =
            seq === -1
              ? { live: true, gt: indexes['offset'].seq }
              : { live: true, gt: seq }
          recordStream = toPull(log.stream(opts))
        }

        return recordStream
      }),
      pull.flatten(),
      pull.filter((record) => isValueOk([op], record.value)),
      pull.map((record) => bipf.decode(record.value, 0))
    )
  }

  return {
    onReady,
    paginate,
    all,
    live,

    // testing
    indexes,
  }
}
