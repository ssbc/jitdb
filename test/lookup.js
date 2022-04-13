// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

const test = require('tape')
const pull = require('pull-stream')
const Pushable = require('pull-pushable')
const validate = require('ssb-validate')
const ssbKeys = require('ssb-keys')
const { prepareAndRunTest, addMsg, helpers } = require('./common')()
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const {
  query,
  and,
  or,
  not,
  equal,
  where,
  slowEqual,
  includes,
  slowIncludes,
  gt,
  gte,
  lt,
  lte,
  deferred,
  liveSeqs,
  seqs,
  offsets,
  fromDB,
  paginate,
  batch,
  startFrom,
  live,
  count,
  descending,
  sortByArrival,
  asOffsets,
  toCallback,
  toPromise,
  toPullStream,
  toAsyncIter,
  slowPredicate,
  slowAbsent,
  absent,
} = require('../operators')

const dir = '/tmp/jitdb-lookup-api'
rimraf.sync(dir)
mkdirp.sync(dir)

const alice = ssbKeys.generate('ed25519', Buffer.alloc(32, 'a'))
const bob = ssbKeys.generate('ed25519', Buffer.alloc(32, 'b'))

prepareAndRunTest('lookup "seq"', dir, (t, jitdb, log) => {
  log.append(Buffer.from('hello'), (e1, offset0) => {
    log.append(Buffer.from('world'), (e2, offset1) => {
      log.append(Buffer.from('foobar'), (e3, offset2) => {
        log.onDrain(() => {
          jitdb.lookup('seq', 0, (err, offset) => {
            t.error(err, 'no error')
            t.equals(offset, offset0)
            jitdb.lookup('seq', 1, (err, offset) => {
              t.error(err, 'no error')
              t.equals(offset, offset1)
              jitdb.lookup('seq', 2, (err, offset) => {
                t.error(err, 'no error')
                t.equals(offset, offset2)
                t.end()
              })
            })
          })
        })
      })
    })
  })
})

prepareAndRunTest('lookup operation', dir, (t, jitdb, log) => {
  const msg1 = { type: 'post', text: '1st', animals: ['cat', 'dog', 'bird'] }
  const msg2 = { type: 'contact', text: '2nd', animals: ['bird'] }
  const msg3 = { type: 'post', text: '3rd', animals: ['cat'] }

  let state = validate.initial()
  state = validate.appendNew(state, null, alice, msg1, Date.now())
  state = validate.appendNew(state, null, alice, msg2, Date.now() + 1)
  state = validate.appendNew(state, null, alice, msg3, Date.now() + 2)

  addMsg(state.queue[0].value, log, (e1, m1) => {
    addMsg(state.queue[1].value, log, (e2, m2) => {
      addMsg(state.queue[2].value, log, (e3, m3) => {
        const op = slowEqual('value.author', 'whatever', {
          prefix: 32,
          indexType: 'value_author',
        })
        jitdb.prepare(op, (err) => {
          t.error(err, 'no error')
          jitdb.lookup(op, 0, (err, authorAsUint32LE) => {
            t.error(err, 'no error')
            const buf = Buffer.alloc(4)
            buf.writeUInt32LE(authorAsUint32LE)
            const prefix = buf.toString('ascii')
            t.equals(prefix, alice.id.slice(0, 4))
            t.end()
          })
        })
      })
    })
  })
})
