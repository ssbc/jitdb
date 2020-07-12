const bipf = require('bipf')
const TypedFastBitSet = require('typedfastbitset')
const RAF = require('polyraf')
const path = require('path')
const push = require('push-stream')
const sanitize = require("sanitize-filename")
const debounce = require('lodash.debounce')

module.exports = function (db, indexesPath) {
  // FIXME: use atomic-file instead?
  function overwrite(filename, seq, count, dataBuffer, cb) {
    console.log("writing index to", filename)

    var b = Buffer.alloc(8)
    b.writeInt32LE(seq, 0)
    b.writeInt32LE(count, 4)

    var file = RAF(filename)
    if (file.deleteable) {
      file.destroy(() => {
        file = RAF(filename)
        file.write(0, b, () => {
          file.write(8, dataBuffer, cb)
        })
      })
    } else {
      file.write(0, b, () => {
        file.write(8, dataBuffer, cb)
      })
    }
  }

  function saveTypedArray(name, seq, count, arr, cb) {
    overwrite(path.join(indexesPath, name + ".index"),
              seq, count, Buffer.from(arr.buffer), cb)
  }

  // FIXME: terminology of what is an index
  function saveIndex(name, seq, index, cb) {
    console.log("saving index:" + name)
    saveTypedArray(name, seq, index.count, index.words)
  }

  function loadIndex(file, Type, cb) {
    const f = RAF(file)
    f.stat((err, stat) => {
      f.read(0, 8, (err, seqCountBuffer) => {
        if (err) return cb(err)
        const seq = seqCountBuffer.readInt32LE(0)
        const count = seqCountBuffer.readInt32LE(4)
        f.read(8, stat.size - 8, (err, buf) => {
          if (err) return cb(err)
          else cb(null, {
            seq,
            count,
            data: new Type(buf.buffer, buf.offset,
                           buf.byteLength / 4)
          })
        })
      })
    })
  }

  var indexes = {}

  function listDirChrome(fs, path, files, cb)
  {
    fs.root.getDirectory(path, {}, function(dirEntry) {
      var dirReader = dirEntry.createReader()
      dirReader.readEntries(function(entries) {
        for (var i = 0; i < entries.length; i++) {
          var entry = entries[i]
          if (entry.isFile)
            files.push(entry.name)
        }
        cb(null, files)
      })
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
            loadIndex(path.join(indexesPath, file), Float32Array, (err, data) => {
              indexes[indexName] = data
              cb()
            })
          }
          else if (file.endsWith(".index")) {
            indexes[indexName] = {
              seq: 0,
              data: new TypedFastBitSet()
            }

            loadIndex(path.join(indexesPath, file), Uint32Array, (err, i) => {
              indexes[indexName].seq = i.seq
              indexes[indexName].data.words = i.data
              indexes[indexName].data.count = i.count
              cb()
            })
          } else
            cb()
        }),
        push.collect(cb)
      )
    }

    if (typeof window !== 'undefined') { // browser
      window.webkitRequestFileSystem(window.PERSISTENT, 0, function (fs) {
        listDirChrome(fs, indexesPath, [], parseIndexes)
      })
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
    console.log("loaded indexes", Object.keys(indexes))

    if (!indexes['offset']) {
      indexes['offset'] = {
        seq: 0,
        count: 0,
        data: new Uint32Array(1000 * 1000) // FIXME: fixed size
      }
    }
    if (!indexes['timestamp']) {
      indexes['timestamp'] = {
        seq: 0,
        count: 0,
        data: new Float32Array(1000 * 1000) // FIXME: fixed size
      }
    }

    isReady = true
    for (var i = 0; i < waiting.length; ++i)
      waiting[i]()
    waiting = []
  })

  const bTimestamp = Buffer.from('timestamp')
  const bValue = Buffer.from('value')
  const bAuthor = Buffer.from('author')
  const bContent = Buffer.from('content')
  const bType = Buffer.from('type')
  const bRoot = Buffer.from('root')

  function getTop(bitset, limit, cb) {
    console.log("results", bitset.size())
    console.time("get values and sort top " + limit)

    function s(x) {
      return {
        val: x,
        timestamp: indexes['timestamp'].data[x]
      }
    }
    var timestamped = bitset.array().map(s)
    var sorted = timestamped.sort((a, b) => b.timestamp - a.timestamp)

    push(
      push.values(sorted),
      push.take(limit),
      push.asyncMap((sorted, cb) => {
        var seq = indexes['offset'].data[sorted.val]
        db.get(seq, cb)
      }),
      push.collect((err, results) => {
        console.timeEnd("get values and sort top " + limit)
        cb(null, results.map(x => bipf.decode(x, 0)))
      })
    )
  }

  function getAll(bitset, cb) {
    var start = Date.now()
    return push(
      push.values(bitset.array()),
      push.asyncMap((val, cb) => {
        var seq = indexes['offset'].data[val]
        db.get(seq, (err, value) => {
          cb(null, bipf.decode(value, 0))
        })
      }),
      push.collect((err, results) => {
        console.log(`get all: ${Date.now()-start}ms, total items: ${results.length}`)
        cb(err, results)
      })
    )
  }

  function updateOffsetIndex(offset, seq) {
    if (offset > indexes['offset'].count - 1) {
      indexes['offset'].seq = seq
      indexes['offset'].data[offset] = seq
      indexes['offset'].count = offset + 1
      return true
    }
  }

  function updateTimestampIndex(offset, seq, data) {
    if (offset > indexes['timestamp'].count - 1) {
      indexes['timestamp'].seq = seq

      var p = 0 // note you pass in p!
      p = bipf.seekKey(data.value, p, bValue)
      var seekKey = bipf.seekKey(data.value, p, bTimestamp)

      indexes['timestamp'].data[offset] = bipf.decode(data.value, seekKey)
      indexes['timestamp'].count = offset + 1
      return true
    }
  }

  function updateIndexValue(opData, index, buffer, offset) {
    var seekKey = opData.seek(buffer)
    if (opData.value === undefined) {
      if (seekKey === -1) {
        index.data.add(offset)
        return true
      }
    }
    else if (~seekKey && bipf.compareString(buffer, seekKey, opData.value) === 0) {
      index.data.add(offset)
      return true
    }

    return false
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
    const start = Date.now()

    db.stream({ gt: index.seq }).pipe({
      paused: false,
      write: function (data) {
        if (updateOffsetIndex(offset, data.seq))
          updatedOffsetIndex = true

        if (updateTimestampIndex(offset, data.seq, data))
          updatedTimestampIndex = true

        updateIndexValue(op.data, index, data.value, offset)

        offset++
      },
      end: () => {
        var count = offset // incremented at end
        console.log(`time: ${Date.now()-start}ms, total items: ${count}`)

        if (updatedOffsetIndex)
          saveTypedArray('offset', indexes['offset'].seq, count, indexes['offset'].data)

        if (updatedTimestampIndex)
          saveTypedArray('timestamp', indexes['timestamp'].seq, count, indexes['timestamp'].data)

        index.seq = indexes['offset'].seq
        if (index.data.size() > 10) // FIXME: configurable, maybe percentage?
          saveIndex(op.data.indexName, indexes['offset'].seq, index.data)

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
    const start = Date.now()
    
    db.stream({}).pipe({
      paused: false,
      write: function (data) {
        var seq = data.seq
        var buffer = data.value

        if (updateOffsetIndex(offset, seq))
          updatedOffsetIndex = true

        if (updateTimestampIndex(offset, data.seq, data))
          updatedTimestampIndex = true

        missingIndexes.forEach(m => {
          updateIndexValue(m, newIndexes[m.indexName], buffer, offset)
        })

        offset++
      },
      end: () => {
        var count = offset // incremented at end
        console.log(`time: ${Date.now()-start}ms, total items: ${count}`)

        if (updatedOffsetIndex)
          saveTypedArray('offset', indexes['offset'].seq, count, indexes['offset'].data)

        if (updatedTimestampIndex)
          saveTypedArray('timestamp', indexes['timestamp'].seq, count, indexes['timestamp'].data)

        for (var indexName in newIndexes) {
          indexes[indexName] = newIndexes[indexName]
          indexes[indexName].seq = indexes['offset'].seq
          if (indexes[indexName].data.size() > 10) // FIXME: configurable, maybe percentage?
            saveIndex(indexName, indexes['offset'].seq, newIndexes[indexName].data)
        }

        cb()
      }
    })
  }

  return {
    // operation:
    //
    // type  | data
    // ------------
    // EQUAL | { seek, value, indexType }
    // AND   | [operation, operation]
    // OR    | [operation, operation]

    query: function(operation, limit, cb) {
      var missingIndexes = []

      function handleOperations(ops) {
        ops.forEach(op => {
          if (op.type === 'EQUAL') {
            var name = op.data.value === undefined ? '' : sanitize(op.data.value.toString())
            op.data.indexName = op.data.indexType + "_" + name
            if (!indexes[op.data.indexName])
              missingIndexes.push(op.data)
          } else if (op.type === 'AND' || op.type === 'OR')
            handleOperations(op.data)
          else
            console.log("Unknown operator type:" + op.type)
        })
      }

      handleOperations([operation])

      if (missingIndexes.length > 0)
        console.log("missing indexes:", missingIndexes)

      function ensureIndexSync(op, cb) {
        if (db.since.value > indexes[op.data.indexName].seq)
          updateIndex(op, cb)
        else
          cb()
      }

      function getIndexForOperation(op, cb) {
        if (op.type === 'EQUAL') {
          ensureIndexSync(op, () => {
            cb(indexes[op.data.indexName].data)
          })
        }
        else if (op.type === 'AND')
        {
          getIndexForOperation(op.data[0], (op1) => {
            getIndexForOperation(op.data[1], (op2) => {
              cb(op1.new_intersection(op2))
            })
          })
        }
        else if (op.type === 'OR')
        {
          getIndexForOperation(op.data[0], (op1) => {
            getIndexForOperation(op.data[1], (op2) => {
              cb(op1.new_union(op2))
            })
          })
        }
      }

      function onIndexesReady() {
        getIndexForOperation(operation, data => {
          if (limit)
            getTop(data, limit, cb)
          else
            getAll(data, cb)
        })
      }

      if (missingIndexes.length > 0)
        createIndexes(missingIndexes, onIndexesReady)
      else
        onIndexesReady()
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

      // FIXME: refactor
      var name = op.data.value === undefined ? '' : sanitize(op.data.value.toString())
      op.data.indexName = op.data.indexType + "_" + name

      var index = indexes[op.data.indexName]
      var offset = 0
      var seq = 0

      if (!index) {
        index = indexes[op.data.indexName] = {
          seq: 0,
          data: new TypedFastBitSet()
        }
      } else {
        seq = index.seq
        // find the next possible offset
        for (; offset < indexes['offset'].data.length; ++offset)
          if (indexes['offset'].data[offset] === index.seq) {
            offset++
            break
          }
      }

      var opts = { live: true }
      if (seq != 0)
        opts.gt = seq

      db.stream(opts).pipe({
        paused: false,
        write: function (data) {
          if (updateIndexValue(op.data, index, data.value, offset))
            syncNewValue(bipf.decode(data.value, 0))

          offset++
        }
      })
    },

    onReady: function(cb) {
      if (isReady)
        cb()
      else waiting.push(cb)
    },

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
    }
  }
}
