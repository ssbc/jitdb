const bipf = require('bipf')
const TypedFastBitSet = require('typedfastbitset')
const path = require('path')
const push = require('push-stream')
const sanitize = require("sanitize-filename")
const debounce = require('lodash.debounce')
const AtomicFile = require('atomic-file/buffer')
const toBuffer = require('typedarray-to-buffer')
const bsb = require('binary-search-bounds')
const debug = require('debug')("jitdb")

module.exports = function (db, indexesPath) {
  function saveTypedArray(name, seq, count, arr, cb) {
    const filename = path.join(indexesPath, name + ".index")
    if (!cb)
      cb = () => {}

    debug("writing index to", filename)

    const dataBuffer = toBuffer(arr)
    var b = Buffer.alloc(8 + dataBuffer.length)
    b.writeInt32LE(seq, 0)
    b.writeInt32LE(count, 4)
    dataBuffer.copy(b, 8)

    var f = AtomicFile(filename)
    f.set(b, cb)
  }

  function saveIndex(name, seq, data, cb) {
    debug("saving index:" + name)
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
        data: new Type(buf.buffer, buf.offset,
                       buf.byteLength /
                       (Type === Float64Array ? 8 : 4))
      })
    })
  }

  var indexes = {}

  function listIndexesIDB(indexesPath, cb) {
    const IdbKvStore = require('idb-kv-store')
    const store = new IdbKvStore(indexesPath, { disableBroadcast: true })
    store.keys(cb)
  }

  function loadLazyIndex(indexName, cb) {
    debug("lazy loading", indexName)
    let index = indexes[indexName]
    loadIndex(index.filepath, Uint32Array, (err, i) => {
      index.seq = i.seq
      index.data.words = i.data
      index.data.count = i.count
      index.lazy = false
      cb()
    })
  }

  function loadIndexes(cb) {
    function parseIndexes(err, files) {
      push(
        push.values(files),
        push.asyncMap((file, cb) => {
          const indexName = file.replace(/\.[^/.]+$/, "")
          if (file === 'offset.index') {
            loadIndex(path.join(indexesPath, file), Uint32Array, (err, data) => {
              indexes[indexName] = data
              cb()
            })
          }
          else if (file === 'timestamp.index') {
            loadIndex(path.join(indexesPath, file), Float64Array, (err, data) => {
              indexes[indexName] = data
              cb()
            })
          }
          else if (file === 'sequence.index') {
            loadIndex(path.join(indexesPath, file), Uint32Array, (err, data) => {
              indexes[indexName] = data

              cb()
            })
          }
          else if (file.endsWith(".index")) {
            indexes[indexName] = {
              seq: 0,
              data: new TypedFastBitSet(),
              lazy: true,
              filepath: path.join(indexesPath, file)
            }

            cb()
          } else
            cb()
        }),
        push.collect(cb)
      )
    }

    if (typeof window !== 'undefined') { // browser
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
    debug("loaded indexes", Object.keys(indexes))

    if (!indexes['offset']) {
      indexes['offset'] = {
        seq: 0,
        count: 0,
        data: new Uint32Array(16 * 1000)
      }
    }
    if (!indexes['timestamp']) {
      indexes['timestamp'] = {
        seq: 0,
        count: 0,
        data: new Float64Array(16 * 1000)
      }
    }
    if (!indexes['sequence']) {
      indexes['sequence'] = {
        seq: 0,
        count: 0,
        data: new Uint32Array(16 * 1000)
      }
    }

    isReady = true
    for (var i = 0; i < waiting.length; ++i)
      waiting[i]()
    waiting = []
  })

  const bTimestamp = Buffer.from('timestamp')
  const bSequence = Buffer.from('sequence')
  const bValue = Buffer.from('value')
  const bAuthor = Buffer.from('author')
  const bContent = Buffer.from('content')
  const bType = Buffer.from('type')
  const bChannel = Buffer.from('channel')
  const bRoot = Buffer.from('root')

  function getValue(val, cb) {
    var seq = indexes['offset'].data[val]
    db.get(seq, (err, res) => {
      if (err && err.code === 'flumelog:deleted')
        cb()
      else
        cb(err, bipf.decode(res, 0))
    })
  }

  function getTop(bitset, offset, limit, reverse, cb) {
    offset = offset || 0
    if (typeof reverse === 'function') {
      cb = reverse
      reverse = false
    }
    debug("results", bitset.size())
    console.time("get values and sort top " + limit)

    function s(x) {
      return {
        val: x,
        timestamp: indexes['timestamp'].data[x]
      }
    }
    var timestamped = bitset.array().map(s)
    var sorted = timestamped.sort((a, b) => {
      if (reverse)
        return a.timestamp - b.timestamp
      else
        return b.timestamp - a.timestamp
    })

    push(
      push.values(sorted.slice(offset, offset + limit)),
      push.asyncMap((s, cb) => getValue(s.val, cb)),
      push.filter(x => x), // deleted messages
      push.collect((err, results) => {
        console.timeEnd("get values and sort top " + limit)
        cb(null, results)
      })
    )
  }

  function getAll(bitset, cb) {
    var start = Date.now()
    return push(
      push.values(bitset.array()),
      push.asyncMap(getValue),
      push.filter(x => x), // deleted messages
      push.collect((err, results) => {
        debug(`get all: ${Date.now()-start}ms, total items: ${results.length}`)
        cb(err, results)
      })
    )
  }

  function getLargerThanSeq(bitset, dbSeq, cb) {
    var start = Date.now()
    return push(
      push.values(bitset.array()),
      push.filter(val => {
        return indexes['offset'].data[val] > dbSeq
      }),
      push.asyncMap(getValue),
      push.filter(x => x), // deleted messages
      push.collect((err, results) => {
        debug(`get all: ${Date.now()-start}ms, total items: ${results.length}`)
        cb(err, results)
      })
    )
  }

  function growIndex(index, Type) {
    debug("growing index")
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

  function checkValue(opData, index, buffer) {
    const seekKey = opData.seek(buffer)
    if (opData.value === undefined)
      return seekKey === -1
    else if (~seekKey && bipf.compareString(buffer, seekKey, opData.value) === 0)
      return true
    else
      return false
  }

  function updateIndexValue(opData, index, buffer, offset) {
    const status = checkValue(opData, index, buffer)
    if (status)
      index.data.add(offset)

    return status
  }

  function updateAllIndexValue(opData, newIndexes, buffer, offset) {
    const seekKey = opData.seek(buffer)
    const value = sanitize(bipf.decode(buffer, seekKey))
    const indexName = opData.indexType + "_" + value

    if (!newIndexes[indexName]) {
      newIndexes[indexName] = {
        seq: 0,
        data: new TypedFastBitSet()
      }
    }

    newIndexes[indexName].data.add(offset)
  }

  function updateIndex(op, cb) {
    var index = indexes[op.data.indexName]

    // find the next possible offset
    for (var offset = 0; offset < indexes['offset'].data.length; ++offset)
      if (indexes['offset'].data[offset] === index.seq) {
        offset++
        break
      }

    var updatedOffsetIndex = false
    var updatedTimestampIndex = false
    var updatedSequenceIndex = false
    const start = Date.now()

    db.stream({ gt: index.seq }).pipe({
      paused: false,
      write: function (data) {
        if (updateOffsetIndex(offset, data.seq))
          updatedOffsetIndex = true

        if (updateTimestampIndex(offset, data.seq, data.value))
          updatedTimestampIndex = true

        if (updateSequenceIndex(offset, data.seq, data.value))
          updatedSequenceIndex = true

        if (op.data.indexName != 'sequence' && op.data.indexName != 'timestamp')
          updateIndexValue(op.data, index, data.value, offset)

        offset++
      },
      end: () => {
        var count = offset // incremented at end
        debug(`time: ${Date.now()-start}ms, total items: ${count}`)

        if (updatedOffsetIndex)
          saveTypedArray('offset', indexes['offset'].seq, count, indexes['offset'].data)

        if (updatedTimestampIndex)
          saveTypedArray('timestamp', indexes['timestamp'].seq, count, indexes['timestamp'].data)

        if (updatedSequenceIndex)
          saveTypedArray('sequence', indexes['sequence'].seq, count, indexes['sequence'].data)

        index.seq = indexes['offset'].seq
        if (op.data.indexName != 'sequence' && op.data.indexName != 'timestamp')
          saveIndex(op.data.indexName, index.seq, index.data)

        cb()
      }
    })
  }

  function createIndexes(missingIndexes, cb) {
    var newIndexes = {}
    missingIndexes.forEach(m => {
      newIndexes[m.indexName] = {
        seq: 0,
        data: new TypedFastBitSet()
      }
    })

    var offset = 0

    var updatedOffsetIndex = false
    var updatedTimestampIndex = false
    var updatedSequenceIndex = false
    const start = Date.now()
    
    db.stream({}).pipe({
      paused: false,
      write: function (data) {
        var seq = data.seq
        var buffer = data.value

        if (updateOffsetIndex(offset, seq))
          updatedOffsetIndex = true

        if (updateTimestampIndex(offset, data.seq, buffer))
          updatedTimestampIndex = true

        if (updateSequenceIndex(offset, data.seq, data.value))
          updatedSequenceIndex = true

        missingIndexes.forEach(m => {
          if (m.indexAll)
            updateAllIndexValue(m, newIndexes, buffer, offset)
          else
            updateIndexValue(m, newIndexes[m.indexName], buffer, offset)
        })

        offset++
      },
      end: () => {
        var count = offset // incremented at end
        debug(`time: ${Date.now()-start}ms, total items: ${count}`)

        if (updatedOffsetIndex)
          saveTypedArray('offset', indexes['offset'].seq, count, indexes['offset'].data)

        if (updatedTimestampIndex)
          saveTypedArray('timestamp', indexes['timestamp'].seq, count, indexes['timestamp'].data)

        if (updatedSequenceIndex)
          saveTypedArray('sequence', indexes['sequence'].seq, count, indexes['sequence'].data)

        for (var indexName in newIndexes) {
          indexes[indexName] = newIndexes[indexName]
          indexes[indexName].seq = indexes['offset'].seq
          saveIndex(indexName, indexes[indexName].seq, indexes[indexName].data)
        }

        cb()
      }
    })
  }

  function loadLazyIndexes(indexNames, cb) {
    push(
      push.values(indexNames),
      push.asyncMap(loadLazyIndex),
      push.collect(cb)
    )
  }

  function setupIndex(op) {
    const name = op.data.value === undefined ? '' : sanitize(op.data.value.toString())
    if (!op.data.indexName)
      op.data.indexName = op.data.indexType + "_" + name
    if (op.data.value !== undefined)
      op.data.value = Buffer.isBuffer(op.data.value) ? op.data.value : Buffer.from(op.data.value)
  }

  function indexSync(operation, cb) {
    var missingIndexes = []
    var lazyIndexes = []

    function handleOperations(ops) {
      ops.forEach(op => {
        if (op.type === 'EQUAL') {
          setupIndex(op)
          if (!indexes[op.data.indexName])
            missingIndexes.push(op.data)
          else if (indexes[op.data.indexName].lazy)
            lazyIndexes.push(op.data.indexName)
        }
        else if (op.type === 'AND' || op.type === 'OR')
          handleOperations(op.data)
        else if (op.type === 'DATA')
          ;
        else
          debug("Unknown operator type:" + op.type)
      })
    }

    handleOperations([operation])

    if (missingIndexes.length > 0)
      debug("missing indexes:", missingIndexes)

    function ensureIndexSync(op, cb) {
      if (db.since.value > indexes[op.data.indexName].seq)
        updateIndex(op, cb)
      else
        cb()
    }

    function filterIndex(op, filterCheck, cb) {
      ensureIndexSync(op, () => {
        if (op.data.indexName == 'sequence') {
          let t = new TypedFastBitSet()
          indexes['sequence'].data.forEach((d, index) => { if (filterCheck(d, op)) t.add(index) })
          cb(t)
        } else if (op.data.indexName == 'timestamp') {
          let t = new TypedFastBitSet()
          indexes['timestamp'].data.forEach((d, index) => { if (filterCheck(d, op)) t.add(index) })
          cb(t)
        }
      })
    }

    function getBitsetForOperation(op, cb) {
      if (op.type === 'EQUAL') {
        ensureIndexSync(op, () => {
          cb(indexes[op.data.indexName].data)
        })
      }
      else if (op.type === 'GT') {
        filterIndex(op, (d, op) => d > op.data.value, cb)
      }
      else if (op.type === 'GTE') {
        filterIndex(op, (d, op) => d >= op.data.value, cb)
      }
      else if (op.type === 'LT') {
        filterIndex(op, (d, op) => d < op.data.value, cb)
      }
      else if (op.type === 'LTE') {
        filterIndex(op, (d, op) => d <= op.data.value, cb)
      }
      else if (op.type === 'DATA') {
        var offsets = []
        op.seqs.sort((x,y) => x-y)
        for (var o = 0; o < indexes['offset'].data.length; ++o)
        {
          if (bsb.eq(op.seqs, indexes['offset'].data[o]) != -1)
            offsets.push(o)

          if (offsets.length == op.seqs.length)
            break
        }

        cb(new TypedFastBitSet(offsets))
      }
      else if (op.type === 'AND')
      {
        getBitsetForOperation(op.data[0], (op1) => {
          getBitsetForOperation(op.data[1], (op2) => {
            cb(op1.new_intersection(op2))
          })
        })
      }
      else if (op.type === 'OR')
      {
        getBitsetForOperation(op.data[0], (op1) => {
          getBitsetForOperation(op.data[1], (op2) => {
            cb(op1.new_union(op2))
          })
        })
      }
    }

    function step3() {
      getBitsetForOperation(operation, cb)
    }

    function step2() {
      if (missingIndexes.length > 0)
        createIndexes(missingIndexes, step3)
      else
        step3()
    }

    if (lazyIndexes.length > 0)
      loadLazyIndexes(lazyIndexes, step2)
    else
      step2()
  }

  function onReady(cb) {
    if (isReady)
      cb()
    else waiting.push(cb)
  }

  return {
    query: function(operation, offset, limit, reverse, cb) {
      onReady(() => {
        indexSync(operation, data => {
          if (limit)
            getTop(data, offset, limit, reverse, cb)
          else
            getAll(data, offset) // offset = cb
        })
      })
    },

    querySeq: function(operation, seq, cb) {
      onReady(() => {
        indexSync(operation, data => {
          getLargerThanSeq(data, seq, cb)
        })
      })
    },

    getSeq(op) {
      return indexes[op.data.indexName].seq
    },

    liveQuerySingleIndex: function(op, cb) {
      var newValues = []
      var sendNewValues = debounce(function() {
        var v = newValues.slice(0)
        newValues = []
        cb(null, v)
      }, 300)

      function syncNewValue(val) {
        newValues.push(val)
        sendNewValues()
      }

      setupIndex(op)

      var opts = { live: true }

      var index = indexes[op.data.indexName]
      if (index)
        opts.gt = index.seq

      db.stream(opts).pipe({
        paused: false,
        write: function (data) {
          if (checkValue(op.data, index, data.value))
            syncNewValue(bipf.decode(data.value, 0))
        }
      })
    },

    onReady,

    // helpers
    seekAuthor: function(buffer) {
      var p = 0 // note you pass in p!
      p = bipf.seekKey(buffer, p, bValue)

      if (~p)
        return bipf.seekKey(buffer, p, bAuthor)
    },

    seekType: function(buffer) {
      var p = 0 // note you pass in p!
      p = bipf.seekKey(buffer, p, bValue)

      if (~p) {
        p = bipf.seekKey(buffer, p, bContent)
        if (~p)
          return bipf.seekKey(buffer, p, bType)
      }
    },

    seekRoot: function(buffer) {
      var p = 0 // note you pass in p!
      p = bipf.seekKey(buffer, p, bValue)

      if (~p) {
        p = bipf.seekKey(buffer, p, bContent)
        if (~p)
          return bipf.seekKey(buffer, p, bRoot)
      }
    },

    seekPrivate: function(buffer) {
      var p = 0 // note you pass in p!
      p = bipf.seekKey(buffer, p, bValue)

      if (~p) {
        p = bipf.seekKey(buffer, p, Buffer.from('meta'))
        if (~p)
          return bipf.seekKey(buffer, p, Buffer.from('private'))
      }
    },

    seekChannel: function(buffer) {
      var p = 0 // note you pass in p!
      p = bipf.seekKey(buffer, p, bValue)

      if (~p) {
        p = bipf.seekKey(buffer, p, bContent)
        if (~p)
          return bipf.seekKey(buffer, p, bChannel)
      }
    },

    // testing
    saveIndex,
    saveTypedArray,
    loadIndex,
    indexes
  }
}
