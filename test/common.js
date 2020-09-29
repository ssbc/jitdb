const FlumeLog = require('async-flumelog')
const Obv = require('obv')
const bipf = require('bipf')
const hash = require('ssb-keys/util').hash
const path = require('path')
const test = require('tape')

module.exports = function () {
  function getId(msg) {
    return '%'+hash(JSON.stringify(msg, null, 2))
  }

  return {
    addMsg: function(msg, raf, cb) {
      var data = {
        key: getId(msg),
        value: msg,
        timestamp: Date.now()
      }
      var b = Buffer.alloc(bipf.encodingLength(data))
      bipf.encode(data, b, 0)
      raf.append(b, function (err, seq) {
        if (err) cb(err)
        // instead of cluttering the tests with onDrain, we just
        // simulate sync adds here
        else raf.onDrain(() => cb(null, data, seq))
      })
    },

    prepareAndRunTest: function(name, dir, cb)
    {
      let raf = FlumeLog(path.join(dir, name), { blockSize: 64*1024 })
      let db = require('../')(raf, path.join(dir, "indexes" + name))
      db.onReady(() => {
        test(name, (t) => cb(t, db, raf))
      })
    }
  }
}
