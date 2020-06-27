const FlumeLog = require('flumelog-aligned-offset')
const bipf = require('bipf')
const TypedFastBitSet = require('typedfastbitset')
const fs = require('fs')
const path = require('path')
const push = require('push-stream')

module.exports = function (dbPath, indexesPath) {
  var raf = FlumeLog(dbPath, {block: 64*1024})

  function saveTypedArray(name, arr) {
    fs.writeFileSync(path.join(indexesPath, name + ".index"), Buffer.from(arr.buffer))
  }

  function saveIndex(name, index) {
    saveTypedArray(name, index.words)
  }

  function loadIndex(file) {
    var buf = fs.readFileSync(file)
    return new Uint32Array(buf.buffer, buf.offset, buf.byteLength/4)
  }

  // FIXME: need a per index, latest seq

  var indexes = {}
  
  function loadIndexes() {
    const files = fs.readdirSync(indexesPath)
    files.forEach(file => {
      if (file == 'offset.index')
        indexes[path.parse(file).name] = loadIndex(path.join(indexesPath, file))
      else if (file.endsWith(".index")) {
        indexes[path.parse(file).name] = new TypedFastBitSet()
        // FIXME: create PR for this
        indexes[path.parse(file).name].words = loadIndex(path.join(indexesPath, file))
        indexes[path.parse(file).name].count = indexes[path.parse(file).name].words.length
      }
    })
  }

  loadIndexes()

  // FIXME: use the seq for this
  var offsetIndexEmpty = false
  
  if (!indexes['offset']) {
    indexes['offset'] = new Uint32Array(1 * 1000 * 1000) // FIXME: fixed size
    offsetIndexEmpty = true
  }
  
  const bTimestamp = new Buffer('timestamp')
  const bValue = new Buffer('value')

  function sortData(data, queue) {
    var p = 0 // note you pass in p!
    p = bipf.seekKey(data.value, p, bValue)
    var seekKey = bipf.seekKey(data.value, p, bTimestamp)
    
    queue.add(data.seq, data.value, seekKey)
  }

  function getTop10(bitset, cb) {
    var queue = require('./bounded-priority-queue')(10)

    console.log("results", bitset.size())
    console.time("get values and sort top 10")

    push(
      push.values(bitset.array()),
      push.asyncMap((val, cb) => {
        var seq = indexes['offset'][val]
        raf.get(seq, (err, value) => {
          sortData({ seq, value }, queue)
          cb()
        })
      }),
      push.collect(() => {
        console.timeEnd("get values and sort top 10")
        cb(null, queue.sorted.map(x => bipf.decode(x.value, 0)))
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
    
    raf.stream({}).pipe({
      paused: false,
      write: function (data) {
        var seq = data.seq
        var buffer = data.value

        if (offsetIndexEmpty)
          indexes['offset'][count] = seq

        missingIndexes.forEach(m => {
          var seekKey = m.seek(buffer)
          if (~seekKey && bipf.compareString(buffer, seekKey, m.value) === 0)
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
          saveIndex(indexName, newIndexes[indexName])
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
    // EQUAL | { seek, value, indexName }
    // AND   | [operation, ...]
    // OR    | [operation, ...]

    query: function(operations, cb) {
      var missingIndexes = []

      function handleOperations(ops) {
        ops.forEach(op => {
          if (op.type == 'EQUAL' && !indexes[op.data.indexName])
            missingIndexes.push(op.data)
          else if (op.type == 'AND' || op.type == 'OR')
            handleOperations(op.data)
          else
            console.log("Unknown operator type:" + op.type)
        })
      }

      handleOperations(operations)

      console.log(missingIndexes)

      function onIndexesReady() {
        operations.forEach(op => {
          // FIXME: recursion
          if (op.type == 'EQUAL')
            getTop10(indexes[op.data.indexName], cb)
          else if (op.type == 'AND')
          {
            // FIXME
            var bitset = indexes[op.data[0].data.indexName].new_intersection(indexes[op.data[1].data.indexName])
            getTop10(bitset, cb)
          }
          else if (op.type == 'OR')
          {
            var bitset = indexes[op.data[0].data.indexName].new_union(indexes[op.data[1].data.indexName])
            getTop10(bitset, cb)
          }
        })
      }

      if (missingIndexes.length > 0)
        createIndexes(missingIndexes, onIndexesReady)
      else
        onIndexesReady()
    },

    // FIXME: something like an index watch
    // useful for contacts index e.g.
  }
}
