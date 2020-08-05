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
var keys3 = ssbKeys.loadOrCreateSync(path.join(dir, 'secret3'))

prepareAndRunTest('Base', dir, (t, db, raf) => {
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
    addMsg(state.queue[1].value, raf, (err, msg2) => {
      db.query(typeQuery, 0, 10, (err, results) => {
        t.equal(results.length, 2)

        // rerun on created index
        db.query(typeQuery, 0, 10, (err, results) => {
          t.equal(results.length, 2)

          const authorQuery = {
            type: 'EQUAL',
            data: {
              seek: db.seekAuthor,
              value: keys.id,
              indexType: "author"
            }
          }
          db.query(authorQuery, 0, 10, (err, results) => {
            t.equal(results.length, 1)
            t.equal(results[0].id, msg1.id)

            // rerun on created index
            db.query(authorQuery, 0, 10, (err, results) => {
              t.equal(results.length, 1)
              t.equal(results[0].id, msg1.id)

              db.query({
                type: 'AND',
                data: [authorQuery, typeQuery]
              }, 0, 10, (err, results) => {
                t.equal(results.length, 1)
                t.equal(results[0].id, msg1.id)

                const authorQuery2 = {
                  type: 'EQUAL',
                  data: {
                    seek: db.seekAuthor,
                    value: keys2.id,
                    indexType: "author"
                  }
                }
                
                db.query({
                  type: 'AND',
                  data: [typeQuery, {
                    type: 'OR',
                    data: [authorQuery, authorQuery2]
                  }]
                }, 0, 10, (err, results) => {
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
      value: 'post',
      indexType: "type"
    }
  }

  addMsg(state.queue[0].value, raf, (err, msg1) => {
    db.query(typeQuery, (err, results) => {
      t.equal(results.length, 1)

      addMsg(state.queue[1].value, raf, (err, msg1) => {
        db.query(typeQuery, (err, results) => {
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
      value: 'post',
      indexType: "type"
    }
  }

  const contactQuery = {
    type: 'EQUAL',
    data: {
      seek: db.seekType,
      value: 'contact',
      indexType: "type"
    }
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    addMsg(state.queue[1].value, raf, (err, msg) => {
      addMsg(state.queue[2].value, raf, (err, msg) => {
        db.query(typeQuery, (err, results) => {
          t.equal(results.length, 2)
          t.equal(results[0].value.content.type, 'post')
          t.equal(results[1].value.content.type, 'post')

          db.query(contactQuery, (err, results) => {
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
      value: 'post',
      indexType: "type"
    }
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    addMsg(state.queue[1].value, raf, (err, msg) => {
      addMsg(state.queue[2].value, raf, (err, msg) => {
        db.query(typeQuery, 0, 1, (err, results) => {
          t.equal(results.length, 1)
          t.equal(results[0].value.content.text, 'Testing 2!')
          t.end()
        })
      })
    })
  })
})

prepareAndRunTest('Offset', dir, (t, db, raf) => {
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
      value: 'post',
      indexType: "type"
    }
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    addMsg(state.queue[1].value, raf, (err, msg) => {
      addMsg(state.queue[2].value, raf, (err, msg) => {
        db.query(typeQuery, 1, 1, (err, results) => {
          t.equal(results.length, 1)
          t.equal(results[0].value.content.text, 'Testing!')
          t.end()
        })
      })
    })
  })
})

prepareAndRunTest('Buffer', dir, (t, db, raf) => {
  const msg1 = { type: 'post', text: 'Testing!' }

  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg1, Date.now())

  const typeQuery = {
    type: 'EQUAL',
    data: {
      seek: db.seekType,
      value: Buffer.from('post'),
      indexType: "type"
    }
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    db.query(typeQuery, 0, 1, (err, results) => {
      t.equal(results.length, 1)
      t.equal(results[0].value.content.text, 'Testing!')
      t.end()
    })
  })
})

prepareAndRunTest('Undefined', dir, (t, db, raf) => {
  const msg1 = { type: 'post', text: 'Testing root', root: '1' }
  const msg2 = { type: 'post', text: 'Testing no root' }

  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg1, Date.now())
  state = validate.appendNew(state, null, keys, msg2, Date.now())

  const typeQuery = {
    type: 'EQUAL',
    data: {
      seek: db.seekRoot,
      value: undefined,
      indexType: "root"
    }
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    addMsg(state.queue[1].value, raf, (err, msg) => {
      db.query(typeQuery, 0, 1, (err, results) => {
        t.equal(results.length, 1)
        t.equal(results[0].value.content.text, 'Testing no root')
        t.end()
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
      value: 'post',
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
      db.query(typeQuery, 0, 1, (err, results) => {
        t.equal(results.length, 1)
        t.equal(results[0].value.content.text, 'Testing 31999')
        t.end()
      })
    })
  )
})

prepareAndRunTest('indexAll', dir, (t, db, raf) => {
  const msg = { type: 'post', text: 'Testing 1' }
  const msgContact = { type: 'contact' }
  const msg2 = { type: 'post', text: 'Testing 2' }
  const msg3 = { type: 'post', text: 'Testing 3' }
  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg, Date.now())
  state = validate.appendNew(state, null, keys, msgContact, Date.now())
  state = validate.appendNew(state, null, keys2, msg2, Date.now())
  state = validate.appendNew(state, null, keys3, msg3, Date.now())

  const authorQuery = {
    type: 'AND',
    data: [
      { type: 'EQUAL',
        data: {
          seek: db.seekType,
          value: 'post',
          indexType: "type"
        }
      },
      { type: 'EQUAL',
        data: {
          seek: db.seekAuthor,
          value: keys.id,
          indexType: "author",
          indexAll: true
        }
      }
    ]
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    addMsg(state.queue[1].value, raf, (err, msg) => {
      addMsg(state.queue[2].value, raf, (err, msg) => {
        addMsg(state.queue[3].value, raf, (err, msg) => {
          db.query(authorQuery, (err, results) => {
            t.equal(results.length, 1)
            t.equal(results[0].value.content.text, 'Testing 1')
            t.equal(Object.keys(db.indexes).length, 3+2+1)
            t.end()
          })
        })
      })
    })
  })
})
