const bipf = require('bipf')
const TypedFastBitSet = require('typedfastbitset')
const RAF = require('polyraf')
const path = require('path')
const push = require('push-stream')
const sanitize = require("sanitize-filename")

module.exports = function (db, indexesPath) {

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

  function loadIndex(file, cb) {
    const f = RAF(file)
    f.stat((err, stat) => {
      f.read(0, 8, (err, seqCountBuffer) => {
        if (err) return cb(err)
        const seq = seqCountBuffer.readInt32LE(0)
        const count = seqCountBuffer.readInt32LE(4)
        f.read(8, stat.size-8, (err, buf) => {
          if (err) return cb(err)
          else cb(null, {
            seq,
            count,
            data: new Uint32Array(buf.buffer, buf.offset, buf.byteLength/4)
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
          if (file == 'offset.index') {
            loadIndex(path.join(indexesPath, file), (err, data) => {
              indexes[indexName] = data
              cb()
            })
          }
          else if (file.endsWith(".index")) {
            indexes[indexName] = {
              seq: 0,
              data: new TypedFastBitSet()
            }

            loadIndex(path.join(indexesPath, file), (err, i) => {
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

  function sortData(data, queue) {
    var p = 0 // note you pass in p!
    p = bipf.seekKey(data.value, p, bValue)
    var seekKey = bipf.seekKey(data.value, p, bTimestamp)
    
    queue.add(data.seq, data.value, seekKey)
  }

  function getTop(bitset, limit, cb) {
    var queue = require('./bounded-priority-queue')(limit)

    console.log("results", bitset.size())
    console.time("get values and sort top " + limit)

    push(
      push.values(bitset.array()),
      push.asyncMap((val, cb) => {
        var seq = indexes['offset'].data[val]
        db.get(seq, (err, value) => {
          sortData({ seq, value }, queue)
          cb()
        })
      }),
      push.collect(() => {
        console.timeEnd("get values and sort top " + limit)
        cb(null, queue.sorted.map(x => bipf.decode(x.value, 0)))
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

  function updateOffsetIndex(count, seq) {
    if (count > indexes['offset'].count) {
      indexes['offset'].seq = seq
      indexes['offset'].data[count] = seq
      indexes['offset'].count = count
      return true
    }
  }

  function updateIndexValue(opData, index, buffer, count) {
    var seekKey = opData.seek(buffer)
    if (opData.value === undefined) {
      if (seekKey === -1)
        index.data.add(count)
    }
    else if (~seekKey && bipf.compareString(buffer, seekKey, opData.value) === 0)
      index.data.add(count)
  }

  function updateIndex(op, cb) {
    var index = indexes[op.data.indexName]

    // find count for index seq
    for (var count = 0; count < indexes['offset'].data.length; ++count)
      if (indexes['offset'].data[count] == index.seq)
        break

    var updatedOffsetIndex = false
    const start = Date.now()

    db.stream({ gt: index.seq }).pipe({
      paused: false,
      write: function (data) {
        var seq = data.seq
        var buffer = data.value

        if (updateOffsetIndex(count, seq))
          updatedOffsetIndex = true

        updateIndexValue(op.data, index, buffer, count)

        count++
      },
      end: () => {
        console.log(`time: ${Date.now()-start}ms, total items: ${count}`)

        if (updatedOffsetIndex)
          saveTypedArray('offset', indexes['offset'].seq, count, indexes['offset'].data)

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

    var count = 0

    var updatedOffsetIndex = false
    const start = Date.now()
    
    db.stream({}).pipe({
      paused: false,
      write: function (data) {
        var seq = data.seq
        var buffer = data.value

        if (updateOffsetIndex(count, seq))
          updatedOffsetIndex = true

        missingIndexes.forEach(m => {
          updateIndexValue(m, newIndexes[m.indexName], buffer, count)
        })

        count++
      },
      end: () => {
        console.log(`time: ${Date.now()-start}ms, total items: ${count}`)

        if (updatedOffsetIndex)
          saveTypedArray('offset', indexes['offset'].seq, count, indexes['offset'].data)

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
          if (op.type == 'EQUAL') {
            var name = op.data.value === undefined ? '' : sanitize(op.data.value.toString())
            op.data.indexName = op.data.indexType + "_" + name
            if (!indexes[op.data.indexName])
              missingIndexes.push(op.data)
          } else if (op.type == 'AND' || op.type == 'OR')
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

      function get_index_for_operation(op, cb) {
        if (op.type == 'EQUAL') {
          ensureIndexSync(op, () => {
            cb(indexes[op.data.indexName].data)
          })
        }
        else if (op.type == 'AND')
        {
          get_index_for_operation(op.data[0], (op1) => {
            get_index_for_operation(op.data[1], (op2) => {
              cb(op1.new_intersection(op2))
            })
          })
        }
        else if (op.type == 'OR')
        {
          get_index_for_operation(op.data[0], (op1) => {
            get_index_for_operation(op.data[1], (op2) => {
              cb(op1.new_union(op2))
            })
          })
        }
      }
      
      function onIndexesReady() {
        get_index_for_operation(operation, data => {
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
    }

    // FIXME: something like an index watch
    // useful for contacts index e.g.
  }
}
