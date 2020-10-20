const test = require('tape')
const validate = require('ssb-validate')
const ssbKeys = require('ssb-keys')
const path = require('path')
const { prepareAndRunTest, addMsg } = require('./common')()

const dir = '/tmp/jitdb-query'
require('rimraf').sync(dir)

var keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

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

prepareAndRunTest('GT,GTE,LT,LTE', dir, (t, db, raf) => {
  const msg1 = { type: 'post', text: '1' }
  const msg2 = { type: 'post', text: '2' }
  const msg3 = { type: 'post', text: '3' }
  const msg4 = { type: 'post', text: '4' }

  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg1, Date.now())
  state = validate.appendNew(state, null, keys, msg2, Date.now())
  state = validate.appendNew(state, null, keys, msg3, Date.now())
  state = validate.appendNew(state, null, keys, msg4, Date.now())

  const filterQuery = {
    type: 'AND',
    data: [
      { type: 'GT',
        data: {
          indexName: 'sequence',
          value: 1,
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

  addMsg(state.queue[0].value, raf, (err, dbMsg1) => {
    addMsg(state.queue[1].value, raf, (err, dbMsg2) => {
      addMsg(state.queue[2].value, raf, (err, dbMsg3) => {
        addMsg(state.queue[3].value, raf, (err, dbMsg4) => {
          db.query(filterQuery, (err, results) => {
            t.equal(results.length, 3)
            t.equal(results[0].value.content.text, '2')

            filterQuery.data[0].type = 'GTE'
            db.query(filterQuery, (err, results) => {
              t.equal(results.length, 4)
              t.equal(results[0].value.content.text, '1')

              filterQuery.data[0].type = 'LT'
              filterQuery.data[0].data.value = 3
              db.query(filterQuery, (err, results) => {
                t.equal(results.length, 2)
                t.equal(results[0].value.content.text, '1')

                filterQuery.data[0].type = 'LTE'
                db.query(filterQuery, (err, results) => {
                  t.equal(results.length, 3)
                  t.equal(results[0].value.content.text, '1')

                  filterQuery.data[0].type = 'GT'
                  filterQuery.data[0].data.indexName = 'timestamp'
                  filterQuery.data[0].data.value = dbMsg1.value.timestamp
                  db.query(filterQuery, (err, results) => {
                    t.equal(results.length, 3)
                    t.equal(results[0].value.content.text, '2')

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
})

