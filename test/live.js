const test = require('tape')
const validate = require('ssb-validate')
const ssbKeys = require('ssb-keys')
const path = require('path')
const { prepareAndRunTest, addMsg } = require('./common')()

const dir = '/tmp/jitdb-live'
require('rimraf').sync(dir)

var keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))
var keys2 = ssbKeys.loadOrCreateSync(path.join(dir, 'secret2'))

prepareAndRunTest('Live', dir, (t, db, raf) => {
  const msg = { type: 'post', text: 'Testing!' }
  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg, Date.now())
  state = validate.appendNew(state, null, keys2, msg, Date.now())

  const typeQuery = {
    type: 'EQUAL',
    data: {
      seek: db.seekType,
      value: Buffer.from('post'),
      indexType: "type"
    }
  }

  var i = 1
  db.liveQuerySingleIndex(typeQuery, (err, results) => {
    if (i++ == 1)
    {
      t.equal(results.length, 1)
      t.equal(results[0].id, state.queue[0].value.id)
      addMsg(state.queue[1].value, raf, (err, msg1) => {})
    }
    else {
      t.equal(results.length, 1)
      t.equal(results[0].id, state.queue[1].value.id)
      t.end()
    }
  })
  
  addMsg(state.queue[0].value, raf, (err, msg1) => {
    console.log("waiting for live query")
  })
})

prepareAndRunTest('Live with initial values', dir, (t, db, raf) => {
  const msg = { type: 'post', text: 'Testing!' }
  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg, Date.now())
  state = validate.appendNew(state, null, keys2, msg, Date.now())

  const typeQuery = {
    type: 'EQUAL',
    data: {
      seek: db.seekType,
      value: Buffer.from('post'),
      indexType: "type"
    }
  }

  addMsg(state.queue[0].value, raf, (err, msg1) => {
    // create index
    db.query(typeQuery, 0, (err, results) => {
      t.equal(results.length, 1)

      db.liveQuerySingleIndex(typeQuery, (err, results) => {
        t.equal(results.length, 1)
        t.equal(results[0].id, state.queue[1].value.id)

        // rerun on updated index
        db.query(typeQuery, 0, (err, results) => {
          t.equal(results.length, 2)
          t.end()
        })
      })

      addMsg(state.queue[1].value, raf, (err, msg1) => {
        console.log("waiting for live query")
      })
    })
  })
})
