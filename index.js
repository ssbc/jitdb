const path = require('path')
const bipf = require('bipf')
const push = require('push-stream')
const pull = require('pull-stream')
const toPull = require('push-stream-to-pull-stream')
const pullAsync = require('pull-async')
const TypedFastBitSet = require('typedfastbitset')
const promisify = require('promisify-4loc')
const bsb = require('binary-search-bounds')
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
    let newLength = index.tarr.length * 2
    // FIXME: even better would be to bring back the exponential jumps, but
    // when reaching the end of the log stream, cut off all the empty space
    const diff = Math.min(newLength - index.tarr.length, 64000)
    newLength = index.tarr.length + diff
    debug('growing index by +%d', diff)
    const newArray = new Type(newLength)
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

  function updatePrefixIndex(opData, index, buffer, offset, seq) {
    if (offset > index.count - 1) {
      if (offset > index.tarr.length) growTarrIndex(index, Uint32Array)

      const seekedField = opData.seek(buffer)
      if (seekedField) {
        const buf = bipf.slice(buffer, seekedField)
        index.tarr[offset] = buf.length ? buf.readUInt32LE(0) : 0
      } else {
        index.tarr[offset] = 0
      }
      index.seq = seq
      index.count = offset + 1
      return true
    }
  }

  function updateIndexValue(opData, index, buffer, offset) {
    if (checkValue(opData, buffer)) index.bitset.add(offset)
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
    const index = indexes[op.data.indexName]

    // find the next possible offset
    let offset = 0
    if (index.seq !== -1) {
      for (; offset < indexes['offset'].tarr.length; ++offset)
        if (indexes['offset'].tarr[offset] === index.seq) {
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
          else updateIndexValue(op.data, index, record.value, offset)
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

  function createIndexes(orphanOps, cb) {
    const newIndexes = {}
    orphanOps.forEach((op) => {
      if (op.data.prefix)
        newIndexes[op.data.indexName] = {
          seq: 0,
          count: 0,
          tarr: new Uint32Array(16 * 1000),
          prefix: op.data.prefix,
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

        orphanOps.forEach((op) => {
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
            updateIndexValue(
              op.data,
              newIndexes[op.data.indexName],
              buffer,
              offset
            )
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

  function sanitizeOpData(op) {
    if (!op.data.indexName) {
      if (op.data.prefix) {
        op.data.indexName = safeFilename(op.data.indexType)
      } else {
        const name = op.data.value === undefined ? '' : op.data.value.toString()
        op.data.indexName = safeFilename(op.data.indexType + '_' + name)
      }
    }
    if (op.data.value !== undefined)
      op.data.value = Buffer.isBuffer(op.data.value)
        ? op.data.value
        : Buffer.from(op.data.value)
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
        for (var i = 0; i < indexes['sequence'].count; ++i) {
          if (filterCheck(indexes['sequence'].tarr[i], op)) bitset.add(i)
        }
        cb(bitset)
      } else if (op.data.indexName === 'timestamp') {
        const bitset = new TypedFastBitSet()
        for (var i = 0; i < indexes['timestamp'].count; ++i) {
          if (filterCheck(indexes['timestamp'].tarr[i], op)) bitset.add(i)
        }
        cb(bitset)
      } else {
        debug('filterIndex() is unsupported for %s', op.data.indexName)
      }
    })
  }

  async function matchAgainstPrefix(op, cb) {
    const bitset = new TypedFastBitSet()
    const target = op.data.value
    const targetPrefix = target.readUInt32LE(0)
    const prefixIndex = indexes[op.data.indexName]
    for (let i = 0; i < prefixIndex.count; ++i) {
      if (prefixIndex.tarr[i] === targetPrefix) bitset.add(i)
    }
    const getRec = promisify(getRecord)
    const recs = await Promise.all(bitset.array().map((o) => getRec(o)))
    recs.forEach((rec) => {
      const candidate = bipf.slice(rec.value, op.data.seek(rec.value))
      if (Buffer.compare(candidate, target)) bitset.remove(rec.offset)
    })
    cb(bitset)
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
    if (op.type === 'EQUAL') {
      if (op.data.prefix) {
        ensureIndexSync(op, () => {
          matchAgainstPrefix(op, cb)
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
        const offsets = []
        op.seqs.sort((x, y) => x - y)
        for (var o = 0; o < indexes['offset'].tarr.length; ++o) {
          if (bsb.eq(op.seqs, indexes['offset'].tarr[o]) !== -1) offsets.push(o)

          if (offsets.length === op.seqs.length) break
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
      // to support `query(fromDB(jitdb), toCallback(cb))`, we do GTE>=0
      getBitsetForOperation(
        { type: 'GTE', data: { indexName: 'sequence', value: 0 } },
        cb
      )
    } else console.error('Unknown type', op)
  }

  function executeOperation(operation, cb) {
    const orphanOps = []
    const lazyIndexes = []

    function detectMissingAndLazyIndexes(ops) {
      ops.forEach((op) => {
        if (op.type === 'EQUAL') {
          sanitizeOpData(op)
          if (!indexes[op.data.indexName]) orphanOps.push(op)
          else if (indexes[op.data.indexName].lazy)
            lazyIndexes.push(op.data.indexName)
        } else if (op.type === 'AND' || op.type === 'OR')
          detectMissingAndLazyIndexes(op.data)
        else if (
          op.type === 'OFFSETS' ||
          op.type === 'LIVEOFFSETS' ||
          op.type === 'SEQS'
        );
        else debug('Unknown operator type:' + op.type)
      })
    }

    function getBitset() {
      getBitsetForOperation(operation, cb)
    }

    function createMissingIndexes() {
      if (orphanOps.length > 0) createIndexes(orphanOps, getBitset)
      else getBitset()
    }

    detectMissingAndLazyIndexes([operation])

    if (orphanOps.length > 0) debug('missing indexes: %o', orphanOps)

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
            if (op.type === 'EQUAL') {
              sanitizeOpData(op)
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
          const opts = { live: true, gt: seq }
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
