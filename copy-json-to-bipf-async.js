// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

var pull = require('pull-stream')
var FlumeLog = require('flumelog-offset')
var AsyncLog = require('async-append-only-log')
var binary = require('bipf')
var json = require('flumecodec/json')

// copy an old (flumelog-offset) log (json) to a new async log (bipf)
function copy(oldpath, newpath, cb) {
  var block = 64 * 1024
  var log = FlumeLog(oldpath, { blockSize: block, codec: json })
  var log2 = AsyncLog(newpath, { blockSize: block })

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
        if (err && err !== true) return cb(err)
        if (err) return cb()
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
}

if (require.main === module) {
  if (process.argv[2] === process.argv[3])
    throw new Error('input must !== output')
  else
    copy(process.argv[2], process.argv[3], (err) => {
      if (err) throw err
      else console.log('done')
    })
} else {
  module.exports = copy
}
