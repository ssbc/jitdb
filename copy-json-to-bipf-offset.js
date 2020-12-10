var pull = require('pull-stream')
var FlumeLog = require('flumelog-offset')
var binary = require('@staltz/bipf')
var json = require('flumecodec/json')

var block = 64 * 1024

//copy an old (flumelog-offset) log (json) to a new async log (bipf)

if (process.argv[2] === process.argv[3])
  throw new Error('input must !== output')

var log = FlumeLog(process.argv[2], { blockSize: block, codec: json })
var log2 = FlumeLog(process.argv[3], { blockSize: block })

pull(
  log.stream({ seqs: false, codec: json }),
  pull.map(function (data) {
    var len = binary.encodingLength(data)
    var b = Buffer.alloc(len)
    binary.encode(data, b, 0)
    return b
  }),
  pull.drain(
    (data) => {
      log2.append(data, function () {})
    },
    () => {
      console.log('done')
    }
  )
)
