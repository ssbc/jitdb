// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

const validate = require('ssb-validate')
const ssbKeys = require('ssb-keys')
const path = require('path')
const pify = require('util').promisify
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const { prepareAndRunTest, addMsgPromise, helpers } = require('./common')()
const {
  fromDB,
  where,
  query,
  slowEqual,
  paginate,
  toPullStream,
} = require('../operators')

const dir = '/tmp/jitdb-add'
rimraf.sync(dir)
mkdirp.sync(dir)

var keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

prepareAndRunTest('delete then index', dir, async (t, jitdb, log) => {
  const content1 = { type: 'post', text: 'Testing 1' }
  const content2 = { type: 'post', text: 'Testing 2' }
  const content3 = { type: 'post', text: 'Testing 3' }
  let state = validate.initial()
  state = validate.appendNew(state, null, keys, content1, Date.now())
  state = validate.appendNew(state, null, keys, content2, Date.now() + 1)
  state = validate.appendNew(state, null, keys, content3, Date.now() + 2)

  const query = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekType,
      value: helpers.toBipf('post'),
      indexType: 'type',
      indexName: 'type_post',
    },
  }

  const msg1 = (await addMsgPromise(state.queue[0].value, log)).msg
  const offset2 = (await addMsgPromise(state.queue[1].value, log)).offset
  const msg3 = (await addMsgPromise(state.queue[2].value, log)).msg

  await pify(log.del)(offset2)
  await pify(log.onDeletesFlushed)()

  const answer = await pify(jitdb.paginate)(
    query,
    0,
    2,
    false,
    false,
    'declared'
  )
  t.deepEqual(
    answer.results.map((msg) => msg.value.content.text),
    ['Testing 1', 'Testing 3'],
    'paginate got msg#1 and msg#3'
  )

  const results = await pify(jitdb.all)(query, 0, false, false, 'declared')
  t.deepEqual(
    results.map((msg) => msg.value.content.text),
    ['Testing 1', 'Testing 3'],
    'all got msg#1 and msg#3'
  )
})

prepareAndRunTest('index then delete', dir, async (t, jitdb, log) => {
  const content1 = { type: 'post', text: 'Testing 1' }
  const content2 = { type: 'post', text: 'Testing 2' }
  const content3 = { type: 'post', text: 'Testing 3' }
  let state = validate.initial()
  state = validate.appendNew(state, null, keys, content1, Date.now())
  state = validate.appendNew(state, null, keys, content2, Date.now() + 1)
  state = validate.appendNew(state, null, keys, content3, Date.now() + 2)

  const query = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekType,
      value: helpers.toBipf('post'),
      indexType: 'type',
      indexName: 'type_post',
    },
  }

  const msg1 = (await addMsgPromise(state.queue[0].value, log)).msg
  const offset2 = (await addMsgPromise(state.queue[1].value, log)).offset
  const msg3 = (await addMsgPromise(state.queue[2].value, log)).msg

  await pify(jitdb.prepare)(query)

  await pify(log.del)(offset2)
  await pify(log.onDeletesFlushed)()

  const answer = await pify(jitdb.paginate)(
    query,
    0,
    2,
    false,
    false,
    'declared'
  )
  t.deepEqual(
    answer.results.map((msg) => msg.value.content.text),
    ['Testing 1'],
    'paginate got msg#1'
  )

  const answer2 = await pify(jitdb.paginate)(
    query,
    answer.nextSeq,
    2,
    false,
    false,
    'declared'
  )
  t.deepEqual(
    answer2.results.map((msg) => msg.value.content.text),
    ['Testing 3'],
    'paginate got msg#3'
  )

  const results = await pify(jitdb.all)(query, 0, false, false, 'declared')
  t.deepEqual(
    results.map((msg) => msg.value.content.text),
    ['Testing 1', 'Testing 3'],
    'all got msg#1 and msg#3'
  )
})

prepareAndRunTest('index then delete many', dir, async (t, jitdb, log) => {
  const content1 = { type: 'post', text: 'Testing 1' }
  const content2 = { type: 'post', text: 'Testing 2' }
  const content3 = { type: 'post', text: 'Testing 3' }
  const content4 = { type: 'post', text: 'Testing 4' }
  let state = validate.initial()
  state = validate.appendNew(state, null, keys, content1, Date.now())
  state = validate.appendNew(state, null, keys, content2, Date.now() + 1)
  state = validate.appendNew(state, null, keys, content3, Date.now() + 2)
  state = validate.appendNew(state, null, keys, content4, Date.now() + 2)

  const offset1 = (await addMsgPromise(state.queue[0].value, log)).offset
  const offset2 = (await addMsgPromise(state.queue[1].value, log)).offset
  const msg3 = (await addMsgPromise(state.queue[2].value, log)).msg
  const msg4 = (await addMsgPromise(state.queue[3].value, log)).msg

  await pify(jitdb.prepare)(slowEqual('value.content.type', 'post'))

  await pify(log.del)(offset1)
  await pify(log.del)(offset2)
  await pify(log.onDeletesFlushed)()

  const source = query(
    fromDB(jitdb),
    where(slowEqual('value.content.type', 'post')),
    paginate(2),
    toPullStream()
  )

  const page1 = await pify(source)(null)
  t.deepEqual(
    page1.map((msg) => msg.value.content.text),
    ['Testing 3', 'Testing 4'],
    'non-empty page 1'
  )
})
