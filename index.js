const bipf = require('bipf')
const TypedFastBitSet = require('typedfastbitset')
const RAF = require('polyraf')
const path = require('path')
const push = require('push-stream')
const sanitize = require("sanitize-filename")

module.exports = function (db, indexesPath) {

  function overwrite(filename, buffer, cb) {
    console.log("writing index to", filename)
    var file = RAF(filename)
    if (file.deleteable) {
      file.destroy(() => {
        file = RAF(filename)
        file.write(0, buffer, cb)
      })
    } else
      file.write(0, buffer, cb)
  }

  function saveTypedArray(name, arr, cb) {
    overwrite(path.join(indexesPath, name + ".index"), Buffer.from(arr.buffer), cb)
  }

  function saveIndex(name, index, cb) {
    saveTypedArray(name, index.words)
  }

  function loadIndex(file, cb) {
    const f = RAF(file)
    f.stat((err, stat) => {
      f.read(0, stat.size, (err, buf) => {
        if (err) return cb(err)
        else cb(null, new Uint32Array(buf.buffer, buf.offset, buf.byteLength/4))
      })
    })
  }

  // FIXME: need a per index, latest seq

  var indexes = {}

  function listDirChrome(fs, path, files, cb)
  {
    fs.root.getDirectory(path, {}, function(dirEntry) {
      var dirReader = dirEntry.createReader()
      dirReader.readEntries(function(entries) {
        for(var i = 0; i < entries.length; i++) {
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
            indexes[indexName] = new TypedFastBitSet()
            loadIndex(path.join(indexesPath, file), (err, data) => {
              // FIXME: create PR for this
              indexes[indexName].words = data
              indexes[indexName].count = data.length
              cb()
            })
          }
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
      indexes['offset'] = new Uint32Array(1 * 1000 * 1000) // FIXME: fixed size
      offsetIndexEmpty = true
    }

    isReady = true
    for (var i = 0; i < waiting.length; ++i)
      waiting[i]()
    waiting = []
  })

  // FIXME: use the seq for this
  var offsetIndexEmpty = false
  
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
        var seq = indexes['offset'][val]
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
        var seq = indexes['offset'][val]
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

  function createIndexes(missingIndexes, cb) {
    var newIndexes = {}
    missingIndexes.forEach(m => {
      newIndexes[m.indexName] = new TypedFastBitSet()
    })

    var count = 0
    const start = Date.now()
    
    db.stream({}).pipe({
      paused: false,
      write: function (data) {
        var seq = data.seq
        var buffer = data.value

        if (offsetIndexEmpty)
          indexes['offset'][count] = seq

        missingIndexes.forEach(m => {
          var seekKey = m.seek(buffer)
          if (m.value === undefined) {
            if (seekKey === -1)
              newIndexes[m.indexName].add(count)
          }
          else if (~seekKey && bipf.compareString(buffer, seekKey, m.value) === 0)
            newIndexes[m.indexName].add(count)
        })

        count++
      },
      end: () => {
        console.log(`time: ${Date.now()-start}ms, total items: ${count}`)

        if (offsetIndexEmpty)
          saveTypedArray('offset', indexes['offset'])

        for (var indexName in newIndexes) {
          indexes[indexName] = newIndexes[indexName]
          if (indexes[indexName].size() > 10) { // FIXME: configurable, maybe percentage?
            saveIndex(indexName, newIndexes[indexName])
          }
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

      function get_index_for_operation(op) {
        if (op.type == 'EQUAL')
          return indexes[op.data.indexName]
        else if (op.type == 'AND')
        {
          const op1 = get_index_for_operation(op.data[0])
          const op2 = get_index_for_operation(op.data[1])
          return op1.new_intersection(op2)
        }
        else if (op.type == 'OR')
        {
          const op1 = get_index_for_operation(op.data[0])
          const op2 = get_index_for_operation(op.data[1])
          return op1.new_union(op2)
        }
      }
      
      function onIndexesReady() {
        if (limit)
          getTop(get_index_for_operation(operation), limit, cb)
        else
          getAll(get_index_for_operation(operation), cb)
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
