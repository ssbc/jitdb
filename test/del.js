const test = require('tape')
const validate = require('ssb-validate')
const ssbKeys = require('ssb-keys')
const path = require('path')
const { prepareAndRunTest, addMsg } = require('./common')()
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')

const dir = '/tmp/jitdb-add'
rimraf.sync(dir)
mkdirp.sync(dir)

var keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

prepareAndRunTest('Delete', dir, (t, db, raf) => {
  const msg1 = { type: 'post', text: 'Testing 1' }
  const msg2 = { type: 'post', text: 'Testing 2' }
  const msg3 = { type: 'post', text: 'Testing 3' }
  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg1, Date.now())
  state = validate.appendNew(state, null, keys, msg2, Date.now())
  state = validate.appendNew(state, null, keys, msg3, Date.now())

  const typeQuery = {
    type: 'EQUAL',
    data: {
      seek: db.seekType,
      value: 'post',
      indexType: "type"
    }
  }

  addMsg(state.queue[0].value, raf, (err, msg1) => {
    addMsg(state.queue[1].value, raf, (err, msg2, seq2) => {
      addMsg(state.queue[2].value, raf, (err, msg3) => {
        raf.del(seq2, () => {
          db.paginate(typeQuery, 0, 10, false, (err, results) => {
            t.deepEqual(results.data, [msg1, msg3])

            db.all(typeQuery, 0, false, (err, results) => {
              t.deepEqual(results, [msg1, msg3])
              t.end()
            })
          })
        })
      })
    })
  })
})
