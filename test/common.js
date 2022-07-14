// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

const Log = require('async-append-only-log')
const bipf = require('bipf')
const hash = require('ssb-keys/util').hash
const path = require('path')
const test = require('tape')
const fs = require('fs')
const JITDB = require('../index')
const helpers = require('./helpers')

module.exports = function () {
  function getId(msg) {
    return '%' + hash(JSON.stringify(msg, null, 2))
  }

  function addMsg(msgVal, log, cb) {
    const msg = {
      key: getId(msgVal),
      value: msgVal,
      timestamp: Date.now(),
    }
    const buf = bipf.allocAndEncode(msg)
    log.append(buf, (err, offset) => {
      if (err) cb(err)
      // instead of cluttering the tests with onDrain, we just
      // simulate sync adds here
      else log.onDrain(() => cb(null, msg, offset))
    })
  }

  function addMsgPromise(msg, raf) {
    return new Promise((resolve, reject) => {
      addMsg(msg, raf, (err, msg, offset) => {
        if (err) reject(err)
        else resolve({ msg, offset })
      })
    })
  }

  return {
    addMsg,
    addMsgPromise,

    prepareAndRunTest: function (name, dir, cb) {
      fs.closeSync(fs.openSync(path.join(dir, name), 'w')) // touch
      const log = Log(path.join(dir, name), { blockSize: 64 * 1024 })
      const jitdb = JITDB(log, path.join(dir, 'indexes' + name))
      jitdb.onReady(() => {
        test(name, (t) => cb(t, jitdb, log))
      })
    },

    helpers,
  }
}
