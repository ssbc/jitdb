const bipf = require('bipf')
const TypedFastBitSet = require('typedfastbitset')
const path = require('path')
const push = require('push-stream')
const toPull = require('push-stream-to-pull-stream')
const pull = require('pull-stream')
const pullAsync = require('pull-async')
const sanitize = require('sanitize-filename')
const debounce = require('lodash.debounce')
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

  function saveTypedArray(name, seq, count, arr, cb) {
    const filename = path.join(indexesPath, name + '.index')
    if (!cb) cb = () => {}

    debug('writing index to', filename)

    const dataBuffer = toBuffer(arr)
    var b = Buffer.alloc(8 + dataBuffer.length)
    b.writeInt32LE(seq, 0)
    b.writeInt32LE(count, 4)
    dataBuffer.copy(b, 8)

    var f = AtomicFile(filename)
    f.set(b, cb)
  }

  function saveIndex(name, seq, data, cb) {
    debug('saving index:' + name)
    data.trim()
    saveTypedArray(name, seq, data.count, data.words, cb)
  }

  function loadIndex(filename, Type, cb) {
    var f = AtomicFile(filename)
    f.get((err, data) => {
      if (err) return cb(err)

      const seq = data.readInt32LE(0)
      const count = data.readInt32LE(4)
      const buf = data.slice(8)

      cb(null, {
        seq,
        count,
        data: new Type(
          buf.buffer,
          buf.offset,
          buf.byteLength / (Type === Float64Array ? 8 : 4)
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
            loadIndex(
              path.join(indexesPath, file),
              Uint32Array,
              (err, data) => {
                indexes[indexName] = data
                cb()
              }
            )
          } else if (file === 'timestamp.index') {
            loadIndex(
              path.join(indexesPath, file),
              Float64Array,
              (err, data) => {
                indexes[indexName] = data
                cb()
              }
            )
          } else if (file === 'sequence.index') {
            loadIndex(
              path.join(indexesPath, file),
              Uint32Array,
              (err, data) => {
                indexes[indexName] = data

                cb()
              }
            )
          } else if (file.endsWith('.index')) {
            indexes[indexName] = {
              seq: 0,
              data: new TypedFastBitSet(),
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
        data: new Uint32Array(16 * 1000),
      }
    }
    if (!indexes['timestamp']) {
      indexes['timestamp'] = {
        seq: -1,
        count: 0,
        data: new Float64Array(16 * 1000),
      }
    }
    if (!indexes['sequence']) {
      indexes['sequence'] = {
        seq: -1,
        count: 0,
        data: new Uint32Array(16 * 1000),
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

  function growIndex(index, Type) {
    debug('growing index')
    let newArray = new Type(index.data.length * 2)
    newArray.set(index.data)
    index.data = newArray
  }

  function updateOffsetIndex(offset, seq) {
    if (offset > indexes['offset'].count - 1) {
      if (offset > indexes['offset'].data.length)
        growIndex(indexes['offset'], Uint32Array)

      indexes['offset'].seq = seq
      indexes['offset'].data[offset] = seq
      indexes['offset'].count = offset + 1
      return true
    }
  }

  function updateTimestampIndex(offset, seq, buffer) {
    if (offset > indexes['timestamp'].count - 1) {
      if (offset > indexes['timestamp'].data.length)
        growIndex(indexes['timestamp'], Float64Array)

      indexes['timestamp'].seq = seq

      var p = 0 // note you pass in p!
      p = bipf.seekKey(buffer, p, bValue)
      p = bipf.seekKey(buffer, p, bTimestamp)

      // FIXME: maybe min of the two timestamps:
      // https://github.com/ssbc/ssb-backlinks/blob/7a731d03acebcbb84b5fee5f0dcc4f6fef3b8035/emit-links.js#L55
      indexes['timestamp'].data[offset] = bipf.decode(buffer, p)
      indexes['timestamp'].count = offset + 1
      return true
    }
  }

  function updateSequenceIndex(offset, seq, buffer) {
    if (offset > indexes['sequence'].count - 1) {
      if (offset > indexes['sequence'].data.length)
        growIndex(indexes['sequence'], Uint32Array)

      indexes['sequence'].seq = seq

      var p = 0 // note you pass in p!
      p = bipf.seekKey(buffer, p, bValue)
      p = bipf.seekKey(buffer, p, bSequence)

      indexes['sequence'].data[offset] = bipf.decode(buffer, p)
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
    if (status) index.data.add(offset)

    return status
  }

  function updateAllIndexValue(opData, newIndexes, buffer, offset) {
    const seekKey = opData.seek(buffer)
    const value = bipf.decode(buffer, seekKey)
    const indexName = safeFilename(opData.indexType + '_' + value)

    if (!newIndexes[indexName]) {
      newIndexes[indexName] = {
        seq: 0,
        data: new TypedFastBitSet(),
      }
    }

    newIndexes[indexName].data.add(offset)
  }

  function updateIndex(op, cb) {
    var index = indexes[op.data.indexName]

    // find the next possible offset
    var offset = 0
    if (index.seq != -1) {
      for (; offset < indexes['offset'].data.length; ++offset)
        if (indexes['offset'].data[offset] === index.seq) {
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
      write: function (data) {
        if (updateOffsetIndex(offset, data.seq)) updatedOffsetIndex = true

        if (updateTimestampIndex(offset, data.seq, data.value))
          updatedTimestampIndex = true

        if (updateSequenceIndex(offset, data.seq, data.value))
          updatedSequenceIndex = true

        if (indexNeedsUpdate)
          updateIndexValue(op.data, index, data.value, offset)

        offset++
      },
      end: () => {
        var count = offset // incremented at end
        debug(`time: ${Date.now() - start}ms, total items: ${count}`)

        if (updatedOffsetIndex)
          saveTypedArray(
            'offset',
            indexes['offset'].seq,
            count,
            indexes['offset'].data
          )

        if (updatedTimestampIndex)
          saveTypedArray(
            'timestamp',
            indexes['timestamp'].seq,
            count,
            indexes['timestamp'].data
          )

        if (updatedSequenceIndex)
          saveTypedArray(
            'sequence',
            indexes['sequence'].seq,
            count,
            indexes['sequence'].data
          )

        index.seq = indexes['offset'].seq
        if (indexNeedsUpdate)
          saveIndex(op.data.indexName, index.seq, index.data)

        cb()
      },
    })
  }

  function createIndexes(missingIndexes, cb) {
    var newIndexes = {}
    missingIndexes.forEach((m) => {
      newIndexes[m.indexName] = {
        seq: 0,
        data: new TypedFastBitSet(),
      }
    })

    var offset = 0

    var updatedOffsetIndex = false
    var updatedTimestampIndex = false
    var updatedSequenceIndex = false
    const start = Date.now()

    log.stream({}).pipe({
      paused: false,
      write: function (data) {
        var seq = data.seq
        var buffer = data.value

        if (updateOffsetIndex(offset, seq)) updatedOffsetIndex = true

        if (updateTimestampIndex(offset, data.seq, buffer))
          updatedTimestampIndex = true

        if (updateSequenceIndex(offset, data.seq, data.value))
          updatedSequenceIndex = true

        missingIndexes.forEach((m) => {
          if (m.indexAll) updateAllIndexValue(m, newIndexes, buffer, offset)
          else updateIndexValue(m, newIndexes[m.indexName], buffer, offset)
        })

        offset++
      },
      end: () => {
        var count = offset // incremented at end
        debug(`time: ${Date.now() - start}ms, total items: ${count}`)

        if (updatedOffsetIndex)
          saveTypedArray(
            'offset',
            indexes['offset'].seq,
            count,
            indexes['offset'].data
          )

        if (updatedTimestampIndex)
          saveTypedArray(
            'timestamp',
            indexes['timestamp'].seq,
            count,
            indexes['timestamp'].data
          )

        if (updatedSequenceIndex)
          saveTypedArray(
            'sequence',
            indexes['sequence'].seq,
            count,
            indexes['sequence'].data
          )

        for (var indexName in newIndexes) {
          indexes[indexName] = newIndexes[indexName]
          indexes[indexName].seq = indexes['offset'].seq
          saveIndex(indexName, indexes[indexName].seq, indexes[indexName].data)
        }

        cb()
      },
    })
  }

  function loadLazyIndex(indexName, cb) {
    debug('lazy loading', indexName)
    let index = indexes[indexName]
    loadIndex(index.filepath, Uint32Array, (err, i) => {
      index.seq = i.seq
      index.data.words = i.data
      index.data.count = i.count
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
            if (filterCheck(indexes['sequence'].data[i], op)) t.add(i)
          }
          cb(t)
        } else if (op.data.indexName == 'timestamp') {
          let t = new TypedFastBitSet()
          for (var i = 0; i < indexes['timestamp'].count; ++i) {
            if (filterCheck(indexes['timestamp'].data[i], op)) t.add(i)
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
          cb(indexes[op.data.indexName].data)
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
          for (var o = 0; o < indexes['offset'].data.length; ++o) {
            if (bsb.eq(op.seqs, indexes['offset'].data[o]) != -1)
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
    var seq = indexes['offset'].data[offset]
    log.get(seq, (err, res) => {
      if (err && err.code === 'flumelog:deleted') cb()
      else cb(err, bipf.decode(res, 0))
    })
  }

  function getRawData(offset, cb) {
    let seq = indexes['offset'].data[offset]
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
        timestamp: indexes['timestamp'].data[x],
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
          data: results,
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
        return indexes['offset'].data[val] > dbSeq
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
          (err, res) => {
            if (err) cb(err)
            else {
              debug(`getTop: ${res.duration}ms, items: ${res.length}`)
              cb(err, res)
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
          (err, res) => {
            if (err) cb(err)
            else {
              debug(`getAll: ${res.duration}ms, total items: ${res.length}`)
              cb(err, res.data)
            }
          }
        )
      })
    })
  }

  function querySeq(operation, seq, cb) {
    onReady(() => {
      indexSync(operation, (data) => {
        getLargerThanSeq(data, seq, cb)
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
        let dataStream

        function setupOps(ops) {
          ops.forEach((op) => {
            if (op.type === 'EQUAL') {
              sanitizeOpData(op)
              if (!indexes[op.data.indexName]) seq = -1
              else seq = indexes[op.data.indexName].seq
            } else if (op.type === 'AND' || op.type === 'OR') setupOps(op.data)
            else if (op.type === 'LIVEOFFSETS') {
              if (dataStream)
                throw new Error('Only one deferred in live supported')
              dataStream = op.stream
            }
          })
        }

        setupOps([op])

        // there are two cases here:

        // - op contains a live deferred, in which case we let the
        //   deferred stream drive new values
        // - op doesn't, in which we let the log stream drive new
        //   values

        if (!dataStream) {
          let opts = { live: true, gt: seq }
          dataStream = toPull(log.stream(opts))
        } else {
          dataStream = pull(
            dataStream,
            pull.asyncMap((i, cb) => {
              ensureOffsetIndexSync(() => {
                getRawData(i, cb)
              })
            })
          )
        }

        return dataStream
      }),
      pull.flatten(),
      pull.filter((data) => isValueOk([op], data.value)),
      pull.map((data) => bipf.decode(data.value, 0))
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
