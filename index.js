const bipf = require('bipf')
const TypedFastBitSet = require('typedfastbitset')
const path = require('path')
const push = require('push-stream')
const toPull = require('push-stream-to-pull-stream')
const pull = require('pull-stream')
const pullAsync = require('pull-async')
const sanitize = require('sanitize-filename')
const AtomicFile = require('atomic-file/buffer')
const toBuffer = require('typedarray-to-buffer')
const bsb = require('binary-search-bounds')
const debug = require('debug')('jitdb')
const jsesc = require('jsesc')

module.exports = function (log, indexesPath) {
  debug('indexes path', indexesPath)

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

  function saveTypedArray(name, seq, count, tarr, cb) {
    const filename = path.join(indexesPath, name + '.index')
    if (!cb) cb = () => {}

    debug('writing index to %s', filename)

    const dataBuffer = toBuffer(tarr)
    var b = Buffer.alloc(8 + dataBuffer.length)
    b.writeInt32LE(seq, 0)
    b.writeInt32LE(count, 4)
    dataBuffer.copy(b, 8)

    var f = AtomicFile(filename)
    f.set(b, cb)
  }

  function saveCoreIndex(name, coreIndex, count) {
    debug('saving index: %s', name)
    saveTypedArray(name, coreIndex.seq, count, coreIndex.tarr)
  }

  function saveIndex(name, seq, bitset, cb) {
    debug('saving index: %s', name)
    bitset.trim()
    saveTypedArray(name, seq, bitset.count, bitset.words, cb)
  }

  function loadIndex(filename, Type, cb) {
    var f = AtomicFile(filename)
    f.get((err, buf) => {
      if (err) return cb(err)

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
  }

  var indexes = {}

  function listIndexesIDB(indexesPath, cb) {
    const IdbKvStore = require('idb-kv-store')
    const store = new IdbKvStore(indexesPath, { disableBroadcast: true })
    store.keys(cb)
  }

  function loadIndexes(cb) {
    function parseIndexes(err, files) {
      push(
        push.values(files),
        push.asyncMap((file, cb) => {
          const indexName = path.parse(file).name
          if (file === 'offset.index') {
            loadIndex(path.join(indexesPath, file), Uint32Array, (e, idx) => {
              indexes[indexName] = idx
              cb()
            })
          } else if (file === 'timestamp.index') {
            loadIndex(path.join(indexesPath, file), Float64Array, (e, idx) => {
              indexes[indexName] = idx
              cb()
            })
          } else if (file === 'sequence.index') {
            loadIndex(path.join(indexesPath, file), Uint32Array, (e, idx) => {
              indexes[indexName] = idx
              cb()
            })
          } else if (file.endsWith('.index')) {
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
      listIndexesIDB(indexesPath, parseIndexes)
    } else {
      const fs = require('fs')
      const mkdirp = require('mkdirp')
      mkdirp.sync(indexesPath)
      parseIndexes(null, fs.readdirSync(indexesPath))
    }
  }

  var isReady = false
  var waiting = []
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

  function growCoreIndex(coreIndex, Type) {
    debug('growing index')
    const newArray = new Type(coreIndex.tarr.length * 2)
    newArray.set(coreIndex.tarr)
    coreIndex.tarr = newArray
  }

  function updateOffsetIndex(offset, seq) {
    if (offset > indexes['offset'].count - 1) {
      if (offset > indexes['offset'].tarr.length)
        growCoreIndex(indexes['offset'], Uint32Array)

      indexes['offset'].seq = seq
      indexes['offset'].tarr[offset] = seq
      indexes['offset'].count = offset + 1
      return true
    }
  }

  function updateTimestampIndex(offset, seq, buffer) {
    if (offset > indexes['timestamp'].count - 1) {
      if (offset > indexes['timestamp'].tarr.length)
        growCoreIndex(indexes['timestamp'], Float64Array)

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
        growCoreIndex(indexes['sequence'], Uint32Array)

      indexes['sequence'].seq = seq

      var p = 0 // note you pass in p!
      p = bipf.seekKey(buffer, p, bValue)
      p = bipf.seekKey(buffer, p, bSequence)

      indexes['sequence'].tarr[offset] = bipf.decode(buffer, p)
      indexes['sequence'].count = offset + 1
      return true
    }
  }

  function checkValue(opData, buffer) {
    const seekKey = opData.seek(buffer)
    if (opData.value === undefined) return seekKey === -1
    else if (
      ~seekKey &&
      bipf.compareString(buffer, seekKey, opData.value) === 0
    )
      return true
    else return false
  }

  function updateIndexValue(opData, index, buffer, offset) {
    const status = checkValue(opData, buffer)
    if (status) index.bitset.add(offset)

    return status
  }

  function updateAllIndexValue(opData, newIndexes, buffer, offset) {
    const seekKey = opData.seek(buffer)
    const value = bipf.decode(buffer, seekKey)
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
    var index = indexes[op.data.indexName]

    // find the next possible offset
    var offset = 0
    if (index.seq != -1) {
      for (; offset < indexes['offset'].tarr.length; ++offset)
        if (indexes['offset'].tarr[offset] === index.seq) {
          offset++
          break
        }
    }

    var updatedOffsetIndex = false
    var updatedTimestampIndex = false
    var updatedSequenceIndex = false
    const start = Date.now()

    const indexNeedsUpdate =
      op.data.indexName != 'sequence' &&
      op.data.indexName != 'timestamp' &&
      op.data.indexName != 'offset'

    log.stream({ gt: index.seq }).pipe({
      paused: false,
      write: function (record) {
        if (updateOffsetIndex(offset, record.seq)) updatedOffsetIndex = true

        if (updateTimestampIndex(offset, record.seq, record.value))
          updatedTimestampIndex = true

        if (updateSequenceIndex(offset, record.seq, record.value))
          updatedSequenceIndex = true

        if (indexNeedsUpdate)
          updateIndexValue(op.data, index, record.value, offset)

        offset++
      },
      end: () => {
        var count = offset // incremented at end
        debug(`time: ${Date.now() - start}ms, total items: ${count}`)

        if (updatedOffsetIndex)
          saveCoreIndex('offset', indexes['offset'], count)

        if (updatedTimestampIndex)
          saveCoreIndex('timestamp', indexes['timestamp'], count)

        if (updatedSequenceIndex)
          saveCoreIndex('sequence', indexes['sequence'], count)

        index.seq = indexes['offset'].seq
        if (indexNeedsUpdate)
          saveIndex(op.data.indexName, index.seq, index.bitset)

        cb()
      },
    })
  }

  function createIndexes(missingIndexes, cb) {
    const newIndexes = {}
    missingIndexes.forEach((m) => {
      newIndexes[m.indexName] = {
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

        missingIndexes.forEach((m) => {
          if (m.indexAll) updateAllIndexValue(m, newIndexes, buffer, offset)
          else updateIndexValue(m, newIndexes[m.indexName], buffer, offset)
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
          saveIndex(indexName, index.seq, index.bitset)
        }

        cb()
      },
    })
  }

  function loadLazyIndex(indexName, cb) {
    debug('lazy loading %s', indexName)
    let index = indexes[indexName]
    loadIndex(index.filepath, Uint32Array, (err, idx) => {
      index.seq = idx.seq
      index.bitset.words = idx.tarr
      index.bitset.count = idx.count
      index.lazy = false
      cb()
    })
  }

  function loadLazyIndexes(indexNames, cb) {
    push(
      push.values(indexNames),
      push.asyncMap(loadLazyIndex),
      push.collect(cb)
    )
  }

  function sanitizeOpData(op) {
    if (!op.data.indexName) {
      const name = op.data.value === undefined ? '' : op.data.value.toString()
      op.data.indexName = safeFilename(op.data.indexType + '_' + name)
    }
    if (op.data.value !== undefined)
      op.data.value = Buffer.isBuffer(op.data.value)
        ? op.data.value
        : Buffer.from(op.data.value)
  }

  function ensureOffsetIndexSync(cb) {
    if (log.since.value > indexes['offset'].seq)
      updateIndex({ data: { indexName: 'offset' } }, cb)
    else cb()
  }

  function indexSync(operation, cb) {
    var missingIndexes = []
    var lazyIndexes = []

    function handleOperations(ops) {
      ops.forEach((op) => {
        if (op.type === 'EQUAL') {
          sanitizeOpData(op)
          if (!indexes[op.data.indexName]) missingIndexes.push(op.data)
          else if (indexes[op.data.indexName].lazy)
            lazyIndexes.push(op.data.indexName)
        } else if (op.type === 'AND' || op.type === 'OR')
          handleOperations(op.data)
        else if (
          op.type === 'OFFSETS' ||
          op.type === 'LIVEOFFSETS' ||
          op.type === 'SEQS'
        );
        else debug('Unknown operator type:' + op.type)
      })
    }

    handleOperations([operation])

    if (missingIndexes.length > 0) debug('missing indexes:', missingIndexes)

    function ensureIndexSync(op, cb) {
      if (log.since.value > indexes[op.data.indexName].seq) updateIndex(op, cb)
      else cb()
    }

    function filterIndex(op, filterCheck, cb) {
      ensureIndexSync(op, () => {
        if (op.data.indexName == 'sequence') {
          let t = new TypedFastBitSet()
          for (var i = 0; i < indexes['sequence'].count; ++i) {
            if (filterCheck(indexes['sequence'].tarr[i], op)) t.add(i)
          }
          cb(t)
        } else if (op.data.indexName == 'timestamp') {
          let t = new TypedFastBitSet()
          for (var i = 0; i < indexes['timestamp'].count; ++i) {
            if (filterCheck(indexes['timestamp'].tarr[i], op)) t.add(i)
          }
          cb(t)
        }
      })
    }

    function nestLargeArray(ops, type) {
      let op = ops[0]

      ops.slice(1).forEach((r) => {
        op = {
          type,
          data: [op, r],
        }
      })

      return op
    }

    function getBitsetForOperation(op, cb) {
      if (op.type === 'EQUAL') {
        ensureIndexSync(op, () => {
          cb(indexes[op.data.indexName].bitset)
        })
      } else if (op.type === 'GT') {
        filterIndex(op, (d, op) => d > op.data.value, cb)
      } else if (op.type === 'GTE') {
        filterIndex(op, (d, op) => d >= op.data.value, cb)
      } else if (op.type === 'LT') {
        filterIndex(op, (d, op) => d < op.data.value, cb)
      } else if (op.type === 'LTE') {
        filterIndex(op, (d, op) => d <= op.data.value, cb)
      } else if (op.type === 'SEQS') {
        ensureOffsetIndexSync(() => {
          var offsets = []
          op.seqs.sort((x, y) => x - y)
          for (var o = 0; o < indexes['offset'].tarr.length; ++o) {
            if (bsb.eq(op.seqs, indexes['offset'].tarr[o]) != -1)
              offsets.push(o)

            if (offsets.length == op.seqs.length) break
          }
          cb(new TypedFastBitSet(offsets))
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
        if (op.data.length > 2) op = nestLargeArray(op.data, 'AND')

        getBitsetForOperation(op.data[0], (op1) => {
          getBitsetForOperation(op.data[1], (op2) => {
            cb(op1.new_intersection(op2))
          })
        })
      } else if (op.type === 'OR') {
        if (op.data.length > 2) op = nestLargeArray(op.data, 'OR')

        getBitsetForOperation(op.data[0], (op1) => {
          getBitsetForOperation(op.data[1], (op2) => {
            cb(op1.new_union(op2))
          })
        })
      } else if (!op.type) {
        getBitsetForOperation(
          { type: 'GTE', data: { indexName: 'sequence', value: 0 } },
          cb
        )
      } else console.error('Unknown type', op)
    }

    function getBitset() {
      getBitsetForOperation(operation, cb)
    }

    function createMissingIndexes() {
      if (missingIndexes.length > 0) createIndexes(missingIndexes, getBitset)
      else getBitset()
    }

    if (lazyIndexes.length > 0)
      loadLazyIndexes(lazyIndexes, createMissingIndexes)
    else createMissingIndexes()
  }

  function isValueOk(ops, value, isOr) {
    for (var i = 0; i < ops.length; ++i) {
      const op = ops[i]
      let ok = false
      if (op.type === 'EQUAL') ok = checkValue(op.data, value)
      else if (op.type === 'AND') ok = isValueOk(op.data, value, false)
      else if (op.type === 'OR') ok = isValueOk(op.data, value, true)
      else if (op.type === 'LIVEOFFSETS') ok = true

      if (ok && isOr) return true
      else if (!ok && !isOr) return false
    }

    if (isOr) return false
    else return true
  }

  function getValue(offset, cb) {
    var seq = indexes['offset'].tarr[offset]
    log.get(seq, (err, res) => {
      if (err && err.code === 'flumelog:deleted') cb()
      else cb(err, bipf.decode(res, 0))
    })
  }

  function getRecord(offset, cb) {
    let seq = indexes['offset'].tarr[offset]
    log.get(seq, (err, res) => {
      if (err && err.code === 'flumelog:deleted') cb()
      else cb(err, { seq, value: res })
    })
  }

  function getValuesFromBitsetSlice(bitset, offset, limit, descending, cb) {
    offset = offset || 0
    var start = Date.now()

    function s(x) {
      return {
        val: x,
        timestamp: indexes['timestamp'].tarr[x],
      }
    }
    var timestamped = bitset.array().map(s)
    var sorted = timestamped.sort((a, b) => {
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
      push.asyncMap((s, cb) => getValue(s.val, cb)),
      push.filter((x) => x), // deleted messages
      push.collect((err, results) => {
        cb(null, {
          results: results,
          total: timestamped.length,
          duration: Date.now() - start,
        })
      })
    )
  }

  function getLargerThanSeq(bitset, dbSeq, cb) {
    var start = Date.now()
    return push(
      push.values(bitset.array()),
      push.filter((val) => {
        return indexes['offset'].tarr[val] > dbSeq
      }),
      push.asyncMap(getValue),
      push.filter((x) => x), // deleted messages
      push.collect((err, results) => {
        debug(
          `get all: ${Date.now() - start}ms, total items: ${results.length}`
        )
        cb(err, results)
      })
    )
  }

  function paginate(operation, offset, limit, descending, cb) {
    onReady(() => {
      indexSync(operation, (bitset) => {
        getValuesFromBitsetSlice(
          bitset,
          offset,
          limit,
          descending,
          (err, answer) => {
            if (err) cb(err)
            else {
              debug(`getTop: ${answer.duration}ms, items: ${answer.length}`)
              cb(err, answer)
            }
          }
        )
      })
    })
  }

  function all(operation, offset, descending, cb) {
    onReady(() => {
      indexSync(operation, (bitset) => {
        getValuesFromBitsetSlice(
          bitset,
          offset,
          null,
          descending,
          (err, answer) => {
            if (err) cb(err)
            else {
              debug(
                `getAll: ${answer.duration}ms, total items: ${answer.length}`
              )
              cb(err, answer.results)
            }
          }
        )
      })
    })
  }

  function querySeq(operation, seq, cb) {
    onReady(() => {
      indexSync(operation, (bitset) => {
        getLargerThanSeq(bitset, seq, cb)
      })
    })
  }

  // live will return new messages as they enter the log
  // can be combined with a normal all or paginate first
  function live(op) {
    return pull(
      pullAsync((cb) =>
        onReady(() => {
          indexSync(op, (bitset) => {
            cb()
          })
        })
      ),
      pull.map(() => {
        let seq = -1
        let recordStream

        function setupOps(ops) {
          ops.forEach((op) => {
            if (op.type === 'EQUAL') {
              sanitizeOpData(op)
              if (!indexes[op.data.indexName]) seq = -1
              else seq = indexes[op.data.indexName].seq
            } else if (op.type === 'AND' || op.type === 'OR') setupOps(op.data)
            else if (op.type === 'LIVEOFFSETS') {
              if (recordStream)
                throw new Error('Only one deferred in live supported')
              recordStream = op.stream
            }
          })
        }

        setupOps([op])

        // there are two cases here:

        // - op contains a live deferred, in which case we let the
        //   deferred stream drive new values
        // - op doesn't, in which we let the log stream drive new
        //   values

        if (!recordStream) {
          let opts = { live: true, gt: seq }
          recordStream = toPull(log.stream(opts))
        } else {
          recordStream = pull(
            recordStream,
            pull.asyncMap((i, cb) => {
              ensureOffsetIndexSync(() => {
                getRecord(i, cb)
              })
            })
          )
        }

        return recordStream
      }),
      pull.flatten(),
      pull.filter((record) => isValueOk([op], record.value)),
      pull.map((record) => bipf.decode(record.value, 0))
    )
  }

  function getSeq(op) {
    return indexes[op.data.indexName].seq
  }

  return {
    onReady,
    paginate,
    all,
    querySeq,
    live,
    getSeq,

    // testing
    saveIndex,
    saveTypedArray,
    loadIndex,
    indexes,
  }
}
