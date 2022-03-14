// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

const fs = require('fs')
const validate = require('ssb-validate')
const ssbKeys = require('ssb-keys')
const path = require('path')
const push = require('push-stream')
const { prepareAndRunTest, addMsg, helpers } = require('./common')()
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')

const dir = '/tmp/jitdb-slow-save'
rimraf.sync(dir)
mkdirp.sync(dir)

var keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

prepareAndRunTest('wip-index-save', dir, (t, db, raf) => {
  if (!process.env.CI) {
    // This test takes at least 1min and is heavy, let's not run it locally
    t.pass('skip this test because we are not in CI')
    t.end()
    return
  }

  t.timeoutAfter(240e3)
  let post = { type: 'post', text: 'Testing' }
  // with our simulated slow log, each msg takes 1.2ms to index
  // so we need at least 50k msgs
  const TOTAL = 90000

  let state = validate.initial()
  for (var i = 0; i < TOTAL; ++i) {
    post.text = 'Testing ' + i
    state = validate.appendNew(state, null, keys, post, Date.now() + i)
  }
  t.pass('generated ' + TOTAL + ' msgs')

  const typeQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekType,
      value: helpers.toBipf('post'),
      indexType: 'type',
      indexName: 'type_post',
    },
  }

  const indexPath = path.join(
    dir,
    'indexes' + 'wip-index-save',
    'type_post.index'
  )

  push(
    push.values(state.queue),
    push.asyncMap((m, cb) => {
      addMsg(m.value, raf, cb)
    }),
    push.collect((err1, results1) => {
      t.error(err1, 'posted ' + TOTAL + ' msgs with no error')
      t.equal(results1.length, TOTAL)

      // Run some empty query to update the core indexes
      db.all({}, 0, false, false, 'declared', (err2, results2) => {
        t.error(err2, 'indexed core with ' + TOTAL + ' msgs with no error')
        t.equal(results2.length, TOTAL)

        let savedAfter1min = false

        // Hack log.stream to make it run slower
        const originalStream = raf.stream
        raf.stream = function (opts) {
          const s = originalStream(opts)
          const originalPipe = s.pipe.bind(s)
          s.pipe = function pipe(o) {
            let originalWrite = o.write
            o.write = (record) => {
              originalWrite(record)
              if (s.sink.paused) t.fail('log.stream didnt respect paused')
              if (!savedAfter1min) {
                s.sink.paused = true
                setTimeout(() => {
                  s.sink.paused = false
                  s.resume()
                }, 1)
              }
            }
            return originalPipe(o)
          }
          return s
        }

        setTimeout(() => {
          t.equal(fs.existsSync(indexPath), false, 'type_post.index isnt saved')
        }, 55e3)

        setTimeout(() => {
          t.equal(fs.existsSync(indexPath), true, 'type_post.index is saved')
          savedAfter1min = true
        }, 65e3)

        // Run an actual query to check if it saves every 1min
        db.all(typeQuery, 0, false, false, 'declared', (err3, results3) => {
          t.error(err3, 'indexed ' + TOTAL + ' msgs no error')
          t.equal(results3.length, TOTAL)

          t.true(savedAfter1min, 'saved after 1 min')
          rimraf.sync(dir) // this folder is quite large, lets save space
          t.end()
        })
      })
    })
  )
})
