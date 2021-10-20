// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

const test = require('tape')
const validate = require('ssb-validate')
const ssbKeys = require('ssb-keys')
const path = require('path')
const { prepareAndRunTest, addMsg, helpers } = require('./common')()
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const bipf = require('bipf')
const { safeFilename } = require('../files')
const { readFile, writeFile } = require('atomic-file-rw')

const dir = '/tmp/jitdb-reindex'
rimraf.sync(dir)
mkdirp.sync(dir)

var keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

function filterStream(raf, filterOffsets) {
  const originalStream = raf.stream
  raf.stream = function (opts) {
    const stream = originalStream(opts)
    const originalPipe = stream.pipe.bind(stream)
    stream.pipe = function (o) {
      const originalWrite = o.write
      o.write = function (record) {
        if (filterOffsets.includes(record.offset)) {
          const v = bipf.decode(record.value, 0)
          v.value.content = 'secret'
          const buf = Buffer.alloc(bipf.encodingLength(v))
          bipf.encode(v, buf, 0)
          record.value = buf
        }
        originalWrite(record)
      }
      return originalPipe(o)
    }
    return stream
  }

  function removeFilter() {
    raf.stream = originalStream
  }

  return removeFilter
}

function addThreeMessages(raf, cb) {
  const msg1 = { type: 'post', text: 'Testing!' }
  const msg2 = { type: 'contact', text: 'Testing!' }
  const msg3 = { type: 'post', text: 'Testing 2!' }

  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg1, Date.now())
  state = validate.appendNew(state, null, keys, msg2, Date.now() + 1)
  state = validate.appendNew(state, null, keys, msg3, Date.now() + 2)

  addMsg(state.queue[0].value, raf, (err, msg) => {
    addMsg(state.queue[1].value, raf, (err, msg) => {
      addMsg(state.queue[2].value, raf, (err, msg) => {
        cb()
      })
    })
  })
}

prepareAndRunTest('reindex seq offset', dir, (t, db, raf) => {
  const typeQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekType,
      value: Buffer.from('post'),
      indexType: 'type',
      indexName: 'type_post',
    },
  }

  addThreeMessages(raf, () => {
    db.all(typeQuery, 0, false, false, (err, results) => {
      t.equal(results.length, 2)
      t.equal(results[0].value.content.type, 'post')
      t.equal(results[1].value.content.type, 'post')

      db.reindex(0, () => {
        db.all(typeQuery, 0, false, false, (err, results) => {
          t.equal(results.length, 2)

          const secondMsgOffset = 352
          db.reindex(secondMsgOffset, () => {
            db.all(typeQuery, 0, false, false, (err, results) => {
              t.equal(results.length, 2)

              t.end()
            })
          })
        })
      })
    })
  })
})

prepareAndRunTest('reindex bitset', dir, (t, db, raf) => {
  const removeFilter = filterStream(raf, [0])

  const typeQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekType,
      value: Buffer.from('post'),
      indexType: 'type',
      indexName: 'type_post',
    },
  }

  addThreeMessages(raf, () => {
    db.all(typeQuery, 0, false, false, (err, results) => {
      t.equal(results.length, 1)
      t.equal(results[0].value.content.type, 'post')

      db.reindex(0, () => {
        removeFilter()
        db.all(typeQuery, 0, false, false, (err, results) => {
          t.equal(results.length, 2)
          t.end()
        })
      })
    })
  })
})

prepareAndRunTest('reindex prefix', dir, (t, db, raf) => {
  const removeFilter = filterStream(raf, [0])

  const typeQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekType,
      value: Buffer.from('post'),
      indexType: 'type',
      indexName: 'type_post_prefix',
      prefix: 32,
      prefixOffset: 0,
    },
  }

  addThreeMessages(raf, () => {
    db.all(typeQuery, 0, false, false, (err, results) => {
      t.equal(results.length, 1)
      t.equal(results[0].value.content.type, 'post')

      db.reindex(0, () => {
        removeFilter()
        db.all(typeQuery, 0, false, false, (err, results) => {
          t.equal(results.length, 2)
          t.end()
        })
      })
    })
  })
})

prepareAndRunTest('reindex prefix map', dir, (t, db, raf) => {
  const removeFilter = filterStream(raf, [0])

  const typeQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekType,
      value: Buffer.from('post'),
      indexType: 'type',
      indexName: 'type_post_prefix',
      prefix: 32,
      prefixOffset: 0,
      useMap: true,
    },
  }

  addThreeMessages(raf, () => {
    db.all(typeQuery, 0, false, false, (err, results) => {
      t.equal(results.length, 1)
      t.equal(results[0].value.content.type, 'post')

      db.reindex(0, () => {
        removeFilter()
        db.all(typeQuery, 0, false, false, (err, results) => {
          t.equal(results.length, 2)
          t.end()
        })
      })
    })
  })
})
