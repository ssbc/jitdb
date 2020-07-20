const test = require('tape')
const validate = require('ssb-validate')
const ssbKeys = require('ssb-keys')
const path = require('path')
const { prepareAndRunTest, addMsg } = require('./common')()
const push = require('push-stream')

const dir = '/tmp/jitdb-add'
require('rimraf').sync(dir)

var keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))
var keys2 = ssbKeys.loadOrCreateSync(path.join(dir, 'secret2'))

prepareAndRunTest('Base', dir, (t, db, raf) => {
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
    addMsg(state.queue[1].value, raf, (err, msg2) => {
      db.query(typeQuery, 10, (err, results) => {
        t.equal(results.length, 2)

        // rerun on created index
        db.query(typeQuery, 10, (err, results) => {
          t.equal(results.length, 2)

          const authorQuery = {
            type: 'EQUAL',
            data: {
              seek: db.seekAuthor,
              value: Buffer.from(keys.id),
              indexType: "author"
            }
          }
          db.query(authorQuery, 10, (err, results) => {
            t.equal(results.length, 1)
            t.equal(results[0].id, msg1.id)

            // rerun on created index
            db.query(authorQuery, 10, (err, results) => {
              t.equal(results.length, 1)
              t.equal(results[0].id, msg1.id)

              db.query({
                type: 'AND',
                data: [authorQuery, typeQuery]
              }, 10, (err, results) => {
                t.equal(results.length, 1)
                t.equal(results[0].id, msg1.id)

                const authorQuery2 = {
                  type: 'EQUAL',
                  data: {
                    seek: db.seekAuthor,
                    value: Buffer.from(keys2.id),
                    indexType: "author"
                  }
                }
                
                db.query({
                  type: 'AND',
                  data: [typeQuery, {
                    type: 'OR',
                    data: [authorQuery, authorQuery2]
                  }]
                }, 10, (err, results) => {
                  t.equal(results.length, 2)
                  t.end()
                })
              })
            })
          })
        })
      })
    })
  })
})

prepareAndRunTest('Update index', dir, (t, db, raf) => {
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
    db.query(typeQuery, 0, (err, results) => {
      t.equal(results.length, 1)

      addMsg(state.queue[1].value, raf, (err, msg1) => {
        db.query(typeQuery, 0, (err, results) => {
          t.equal(results.length, 2)
          t.end()
        })
      })
    })
  })
})

prepareAndRunTest('Multiple types', dir, (t, db, raf) => {
  const msg1 = { type: 'post', text: 'Testing!' }
  const msg2 = { type: 'contact', text: 'Testing!' }
  const msg3 = { type: 'post', text: 'Testing 2!' }

  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg1, Date.now())
  state = validate.appendNew(state, null, keys, msg2, Date.now())
  state = validate.appendNew(state, null, keys, msg3, Date.now())

  const typeQuery = {
    type: 'EQUAL',
    data: {
      seek: db.seekType,
      value: Buffer.from('post'),
      indexType: "type"
    }
  }

  const contactQuery = {
    type: 'EQUAL',
    data: {
      seek: db.seekType,
      value: Buffer.from('contact'),
      indexType: "type"
    }
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    addMsg(state.queue[1].value, raf, (err, msg) => {
      addMsg(state.queue[2].value, raf, (err, msg) => {
        db.query(typeQuery, 0, (err, results) => {
          t.equal(results.length, 2)
          t.equal(results[0].value.content.type, 'post')
          t.equal(results[1].value.content.type, 'post')

          db.query(contactQuery, 0, (err, results) => {
            t.equal(results.length, 1)
            t.equal(results[0].value.content.type, 'contact')

            t.end()
          })
        })
      })
    })
  })
})

prepareAndRunTest('Top 1 multiple types', dir, (t, db, raf) => {
  const msg1 = { type: 'post', text: 'Testing!' }
  const msg2 = { type: 'contact', text: 'Testing!' }
  const msg3 = { type: 'post', text: 'Testing 2!' }

  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg1, Date.now())
  state = validate.appendNew(state, null, keys, msg2, Date.now()+1)
  state = validate.appendNew(state, null, keys, msg3, Date.now()+2)

  const typeQuery = {
    type: 'EQUAL',
    data: {
      seek: db.seekType,
      value: Buffer.from('post'),
      indexType: "type"
    }
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    addMsg(state.queue[1].value, raf, (err, msg) => {
      addMsg(state.queue[2].value, raf, (err, msg) => {
        db.query(typeQuery, 1, (err, results) => {
          t.equal(results.length, 1)
          t.equal(results[0].value.content.text, 'Testing 2!')
          t.end()
        })
      })
    })
  })
})

prepareAndRunTest('grow', dir, (t, db, raf) => {
  let msg = { type: 'post', text: 'Testing' }

  let state = validate.initial()
  for (var i = 0; i < 32 * 1000; ++i) {
    msg.text = "Testing " + i
    state = validate.appendNew(state, null, keys, msg, Date.now())
  }

  const typeQuery = {
    type: 'EQUAL',
    data: {
      seek: db.seekType,
      value: Buffer.from('post'),
      indexType: "type"
    }
  }

  push(
    push.values(state.queue),
    push.asyncMap((q, cb) => {
      addMsg(q.value, raf, cb)
    }),
    push.collect((err, results) => {
      console.log("done inserting", results.length)
      db.query(typeQuery, 1, (err, results) => {
        t.equal(results.length, 1)
        t.equal(results[0].value.content.text, 'Testing 31999')
        t.end()
      })
    })
  )
})
