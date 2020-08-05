const test = require('tape')
const validate = require('ssb-validate')
const ssbKeys = require('ssb-keys')
const path = require('path')
const { prepareAndRunTest, addMsg } = require('./common')()

const dir = '/tmp/jitdb-query-seq'
require('rimraf').sync(dir)

var keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))
var keys2 = ssbKeys.loadOrCreateSync(path.join(dir, 'secret2'))

prepareAndRunTest('Basic', dir, (t, db, raf) => {
  const msg = { type: 'post', text: 'Testing!' }
  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg, Date.now())
  state = validate.appendNew(state, null, keys2, msg, Date.now())

  const typeQuery = {
    type: 'EQUAL',
    data: {
      seek: db.seekType,
      value: 'post',
      indexType: "type"
    }
  }

  addMsg(state.queue[0].value, raf, (err, msg1) => {
    db.query(typeQuery, (err, results) => {
      t.equal(results.length, 1)
      t.equal(results[0].id, state.queue[0].value.id)
      const seq1 = db.getSeq(typeQuery)

      db.querySeq(typeQuery, seq1, (err, results) => {
        t.equal(results.length, 0)
        
        addMsg(state.queue[1].value, raf, (err, msg1) => {
          db.querySeq(typeQuery, seq1, (err, results) => {
            t.equal(results.length, 1)
            t.equal(results[0].id, state.queue[1].value.id)
            t.end()
          })
        })
      })
    })
  })
})
