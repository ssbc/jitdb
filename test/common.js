const Log = require('async-log')
const bipf = require('bipf')
const hash = require('ssb-keys/util').hash
const path = require('path')
const test = require('tape')
const fs = require('fs')
const jitdb = require('../index')
const helpers = require('./helpers')

module.exports = function () {
  function getId(msg) {
    return '%' + hash(JSON.stringify(msg, null, 2))
  }

  return {
    addMsg: function (msg, raf, cb) {
      var data = {
        key: getId(msg),
        value: msg,
        timestamp: Date.now(),
      }
      var b = Buffer.alloc(bipf.encodingLength(data))
      bipf.encode(data, b, 0)
      raf.append(b, function (err, offset) {
        if (err) cb(err)
        // instead of cluttering the tests with onDrain, we just
        // simulate sync adds here
        else raf.onDrain(() => cb(null, data, offset))
      })
    },

    prepareAndRunTest: function (name, dir, cb) {
      fs.closeSync(fs.openSync(path.join(dir, name), 'w')) // touch
      let raf = Log(path.join(dir, name), { blockSize: 64 * 1024 })
      let db = jitdb(raf, path.join(dir, 'indexes' + name))
      db.onReady(() => {
        test(name, (t) => cb(t, db, raf))
      })
    },

    helpers,
  }
}
