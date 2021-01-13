const test = require('tape')
const validate = require('ssb-validate')
const ssbKeys = require('ssb-keys')
const path = require('path')
const { prepareAndRunTest, addMsg, helpers } = require('./common')()
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const { safeFilename } = require('../files')

const dir = '/tmp/jitdb-query'
rimraf.sync(dir)
mkdirp.sync(dir)

var keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

prepareAndRunTest('Ensure seq index is updated always', dir, (t, db, raf) => {
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
      indexName: 'type_post',
    },
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    addMsg(state.queue[1].value, raf, (err, msg) => {
      db.all(typeQuery, 0, false, false, (err, results) => {
        t.equal(results.length, 1)
        t.equal(results[0].value.content.type, 'post')

        db.indexes['seq'] = {
          offset: -1,
          count: 0,
          tarr: new Uint32Array(16 * 1000),
        }

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
})
