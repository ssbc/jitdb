const validate = require('ssb-validate')
const ssbKeys = require('ssb-keys')
const path = require('path')
const { prepareAndRunTest, addMsg, helpers } = require('./common')()
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')

const dir = '/tmp/jitdb-prefix'
rimraf.sync(dir)
mkdirp.sync(dir)

var keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

prepareAndRunTest('Prefix equal', dir, (t, db, raf) => {
  const msg1 = { type: 'post', text: 'Testing!' }
  const msg2 = { type: 'contact', text: 'Testing!' }
  const msg3 = { type: 'post', text: 'Testing 2!' }

  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg1, Date.now())
  state = validate.appendNew(state, null, keys, msg2, Date.now() + 1)
  state = validate.appendNew(state, null, keys, msg3, Date.now() + 2)

  const typeQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekType,
      value: Buffer.from('post'),
      indexType: 'type',
      prefix: 32,
    },
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    addMsg(state.queue[1].value, raf, (err, msg) => {
      addMsg(state.queue[2].value, raf, (err, msg) => {
        db.all(typeQuery, 0, false, false, (err, results) => {
          t.equal(results.length, 2)
          t.equal(results[0].value.content.type, 'post')
          t.equal(results[1].value.content.type, 'post')
          t.end()
        })
      })
    })
  })
})

prepareAndRunTest('Prefix larger than actual value', dir, (t, db, raf) => {
  const msg1 = { type: 'post', text: 'First', channel: 'foo' }
  const msg2 = { type: 'contact', text: 'Second' }
  const msg3 = { type: 'post', text: 'Third' }

  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg1, Date.now())
  state = validate.appendNew(state, null, keys, msg2, Date.now() + 1)
  state = validate.appendNew(state, null, keys, msg3, Date.now() + 2)

  const typeQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekChannel,
      value: Buffer.from('foo'),
      indexType: 'channel',
      prefix: 32,
    },
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    addMsg(state.queue[1].value, raf, (err, msg) => {
      addMsg(state.queue[2].value, raf, (err, msg) => {
        db.all(typeQuery, 0, false, false, (err, results) => {
          t.equal(results.length, 1)
          t.equal(results[0].value.content.text, 'First')
          t.end()
        })
      })
    })
  })
})

prepareAndRunTest('Prefix equal falsy', dir, (t, db, raf) => {
  const msg1 = { type: 'post', text: 'First', channel: 'foo' }
  const msg2 = { type: 'contact', text: 'Second' }
  const msg3 = { type: 'post', text: 'Third' }

  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg1, Date.now())
  state = validate.appendNew(state, null, keys, msg2, Date.now() + 1)
  state = validate.appendNew(state, null, keys, msg3, Date.now() + 2)

  const typeQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekChannel,
      value: null,
      indexType: 'channel',
      prefix: 32,
    },
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    addMsg(state.queue[1].value, raf, (err, msg) => {
      addMsg(state.queue[2].value, raf, (err, msg) => {
        db.all(typeQuery, 0, false, false, (err, results) => {
          t.equal(results.length, 2)
          t.equal(results[0].value.content.text, 'Second')
          t.equal(results[1].value.content.text, 'Third')
          t.end()
        })
      })
    })
  })
})
