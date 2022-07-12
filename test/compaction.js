// SPDX-FileCopyrightText: 2022 Andre 'Staltz' Medeiros <contact@staltz.com>
//
// SPDX-License-Identifier: CC0-1.0

const validate = require('ssb-validate')
const ssbKeys = require('ssb-keys')
const path = require('path')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const pify = require('util').promisify
const { prepareAndRunTest, addMsgPromise, helpers } = require('./common')()
const {
  query,
  fromDB,
  where,
  slowEqual,
  paginate,
  toPullStream,
} = require('../operators')

const dir = '/tmp/jitdb-compaction'
rimraf.sync(dir)
mkdirp.sync(dir)

const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

prepareAndRunTest('toPullStream post-compact', dir, async (t, jitdb, log) => {
  const content0 = { type: 'post', text: 'Testing 0' }
  const content1 = { type: 'post', text: 'Testing 1' }
  const content2 = { type: 'post', text: 'Testing 2' }
  const content3 = { type: 'post', text: 'Testing 3' }
  const content4 = { type: 'post', text: 'Testing 4' }
  const content5 = { type: 'post', text: 'Testing 5' }
  const content6 = { type: 'post', text: 'Testing 6' }
  const content7 = { type: 'post', text: 'Testing 7' }
  let state = validate.initial()
  state = validate.appendNew(state, null, keys, content0, Date.now())
  state = validate.appendNew(state, null, keys, content1, Date.now() + 1)
  state = validate.appendNew(state, null, keys, content2, Date.now() + 2)
  state = validate.appendNew(state, null, keys, content3, Date.now() + 3)
  state = validate.appendNew(state, null, keys, content4, Date.now() + 4)
  state = validate.appendNew(state, null, keys, content5, Date.now() + 5)
  state = validate.appendNew(state, null, keys, content6, Date.now() + 6)
  state = validate.appendNew(state, null, keys, content7, Date.now() + 7)

  const typeQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekType,
      value: helpers.toBipf('post'),
      indexType: 'type',
      indexName: 'type_post',
    },
  }

  const offset0 = (await addMsgPromise(state.queue[0].value, log)).offset
  const offset1 = (await addMsgPromise(state.queue[1].value, log)).offset
  const offset2 = (await addMsgPromise(state.queue[2].value, log)).offset
  const offset3 = (await addMsgPromise(state.queue[3].value, log)).offset
  const offset4 = (await addMsgPromise(state.queue[4].value, log)).offset
  const offset5 = (await addMsgPromise(state.queue[5].value, log)).offset
  const offset6 = (await addMsgPromise(state.queue[6].value, log)).offset
  const offset7 = (await addMsgPromise(state.queue[7].value, log)).offset

  await pify(log.del)(offset1)
  t.pass('delete msg 1')
  await pify(log.del)(offset2)
  t.pass('delete msg 2')
  await pify(log.onDeletesFlushed)()

  const source = query(
    fromDB(jitdb),
    where(slowEqual('value.content.type', 'post')),
    paginate(3),
    toPullStream()
  )

  const msgs = await pify(source)(null)
  t.equals(msgs.length, 3, '1st page has 3 messages')
  t.equals(msgs[0].value.content.text, 'Testing 0', 'msg 0')
  t.equals(msgs[1].value.content.text, 'Testing 3', 'msg 3')
  t.equals(msgs[2].value.content.text, 'Testing 4', 'msg 4')

  // FIXME: AAOL needs to set stats.done = false as soon as compact() is called

  let queryStarted = false
  let queryEnded = false
  await new Promise((resolve) => {
    log.compactionProgress((stats) => {
      if (!stats.done && !queryStarted) {
        queryStarted = true
        source(null, (err, msgs) => {
          t.error(err, 'no error')
          t.equals(msgs.length, 3, '2nd page has 3 messages')
          t.equals(msgs[0].value.content.text, 'Testing 5', 'msg 5')
          t.equals(msgs[1].value.content.text, 'Testing 6', 'msg 6')
          t.equals(msgs[2].value.content.text, 'Testing 7', 'msg 7')
          queryEnded = true
        })
        return false // abort listening to compaction progress
      }
    })

    log.compact((err) => {
      if (err) t.fail(err)
      resolve()
    })
  })

  t.true(queryEnded, 'query ended')
})
