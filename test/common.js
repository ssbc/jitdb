const FlumeLog = require('flumelog-aligned-offset')
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
      raf.append(b, false, function (err) {
        if (err) cb(err)
        else cb(null, data)
      })
    },

    prepareAndRunTest: function(name, dir, cb)
    {
      let raf = FlumeLog(path.join(dir, name), {block: 64*1024})
      raf.since = Obv()
      raf.onWrite = raf.since.set

      let db = require('../index')(raf, path.join(dir, "indexes" + name))
      db.onReady(() => {
        test(name, (t) => cb(t, db, raf))
      })
    }
  }
}
