var pull = require('pull-stream')
var FlumeLog = require('flumelog-offset')
var AsyncFlumeLog = require('async-flumelog')
var binary = require('bipf')
var json = require('flumecodec/json')

var block = 64 * 1024

//copy an old (flumelog-offset) log (json) to a new async log (bipf)

if (process.argv[2] === process.argv[3])
  throw new Error('input must !== output')

var log = FlumeLog(process.argv[2], { blockSize: block, codec: json })
var log2 = AsyncFlumeLog(process.argv[3], { blockSize: block })

var dataTransferred = 0

pull(
  log.stream({ seqs: false, codec: json }),
  pull.map(function (data) {
    var len = binary.encodingLength(data)
    var b = Buffer.alloc(len)
    binary.encode(data, b, 0)
    return b
  }),
  function (read) {
    read(null, function next(err, data) {
      if (err && err !== true) throw err
      if (err) return console.error('done')
      dataTransferred += data.length
      log2.append(data, function () {})
      if (dataTransferred % block == 0)
        log2.onDrain(function () {
          read(null, next)
        })
      else read(null, next)
    })
  }
)
