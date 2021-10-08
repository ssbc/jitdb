// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

const validate = require('ssb-validate')
const ssbKeys = require('ssb-keys')
const path = require('path')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const pull = require('pull-stream')
const { addMsg, prepareAndRunTest } = require('./common')()
const {
  where,
  query,
  fromDB,
  live,
  toPullStream,
  slowEqual,
} = require('../operators')

const dir = '/tmp/jitdb-live-then-grow'
rimraf.sync(dir)
mkdirp.sync(dir)

var keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

prepareAndRunTest('Live toPullStream from empty log', dir, (t, db, raf) => {
  const msg1 = { type: 'post', text: '1st' }
  const msg2 = { type: 'post', text: '2nd' }
  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg1, Date.now())
  state = validate.appendNew(state, null, keys, msg2, Date.now())

  var i = 1
  pull(
    query(
      fromDB(db),
      where(slowEqual('value.content.type', 'post')),
      live(),
      toPullStream()
    ),
    pull.drain((result) => {
      if (i++ === 1) {
        t.equal(result.value.content.text, '1st')
        addMsg(state.queue[1].value, raf, (err, m2) => {})
      } else {
        t.equal(result.value.content.text, '2nd')
        t.end()
      }
    })
  )

  addMsg(state.queue[0].value, raf, (err, m1) => {})
})
