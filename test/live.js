const test = require('tape')
const validate = require('ssb-validate')
const ssbKeys = require('ssb-keys')
const path = require('path')
const { prepareAndRunTest, addMsg, helpers } = require('./common')()
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const pull = require('pull-stream')

const dir = '/tmp/jitdb-live'
rimraf.sync(dir)
mkdirp.sync(dir)

var keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))
var keys2 = ssbKeys.loadOrCreateSync(path.join(dir, 'secret2'))
var keys3 = ssbKeys.loadOrCreateSync(path.join(dir, 'secret3'))

prepareAndRunTest('Live', dir, (t, db, raf) => {
  const msg = { type: 'post', text: 'Testing!' }
  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg, Date.now())
  state = validate.appendNew(state, null, keys2, msg, Date.now() + 1)

  const typeQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekType,
      value: Buffer.from('post'),
      indexType: 'type',
      indexName: 'type_post',
    },
  }

  var i = 1
  pull(
    db.live(typeQuery),
    pull.drain((result) => {
      if (i++ == 1) {
        t.equal(result.key, state.queue[0].key)
        addMsg(state.queue[1].value, raf, (err, msg1) => {})
      } else {
        t.equal(result.key, state.queue[1].key)
        t.end()
      }
    })
  )

  addMsg(state.queue[0].value, raf, (err, msg1) => {
    // console.log("waiting for live query")
  })
})

prepareAndRunTest('Live AND', dir, (t, db, raf) => {
  const msg = { type: 'post', text: 'Testing ' + keys.id }
  const msg2 = { type: 'post', text: 'Testing ' + keys2.id }
  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg, Date.now())
  state = validate.appendNew(state, null, keys2, msg2, Date.now() + 1)

  const filterQuery = {
    type: 'AND',
    data: [
      {
        type: 'EQUAL',
        data: {
          seek: helpers.seekAuthor,
          value: Buffer.from(keys.id),
          indexType: 'author',
          indexName: 'author_' + keys.id,
        },
      },
      {
        type: 'EQUAL',
        data: {
          seek: helpers.seekType,
          value: Buffer.from('post'),
          indexType: 'type',
          indexName: 'type_post',
        },
      },
    ],
  }

  var i = 1
  pull(
    db.live(filterQuery),
    pull.drain((result) => {
      if (i++ == 1) {
        t.equal(result.key, state.queue[0].key)
        addMsg(state.queue[1].value, raf, (err, msg1) => {})

        setTimeout(() => {
          t.end()
        }, 500)
      } else {
        t.fail('should only be called once')
      }
    })
  )

  addMsg(state.queue[0].value, raf, (err, msg1) => {
    // console.log("waiting for live query")
  })
})

prepareAndRunTest('Live OR', dir, (t, db, raf) => {
  const msg = { type: 'post', text: 'Testing ' + keys.id }
  const msg2 = { type: 'post', text: 'Testing ' + keys2.id }
  const msg3 = { type: 'post', text: 'Testing ' + keys3.id }
  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg, Date.now())
  state = validate.appendNew(state, null, keys2, msg2, Date.now() + 1)
  state = validate.appendNew(state, null, keys3, msg3, Date.now() + 2)

  const authorQuery = {
    type: 'OR',
    data: [
      {
        type: 'EQUAL',
        data: {
          seek: helpers.seekAuthor,
          value: Buffer.from(keys.id),
          indexType: 'author',
          indexName: 'author_' + keys.id,
        },
      },
      {
        type: 'EQUAL',
        data: {
          seek: helpers.seekAuthor,
          value: Buffer.from(keys2.id),
          indexType: 'author',
          indexName: 'author_' + keys2.id,
        },
      },
    ],
  }

  const filterQuery = {
    type: 'AND',
    data: [
      authorQuery,
      {
        type: 'EQUAL',
        data: {
          seek: helpers.seekType,
          value: Buffer.from('post'),
          indexType: 'type',
          indexName: 'type_post',
        },
      },
    ],
  }

  var i = 1
  pull(
    db.live(filterQuery),
    pull.drain((result) => {
      if (i == 1) {
        t.equal(result.key, state.queue[0].key)
        addMsg(state.queue[1].value, raf, () => {})
      } else if (i == 2) {
        t.equal(result.key, state.queue[1].key)
        addMsg(state.queue[2].value, raf, () => {})

        setTimeout(() => {
          t.end()
        }, 500)
      } else {
        t.fail('should only be called for the first 2')
      }

      i += 1
    })
  )

  addMsg(state.queue[0].value, raf, (err, msg1) => {
    // console.log("waiting for live query")
  })
})

prepareAndRunTest('Live GTE', dir, (t, db, raf) => {
  const msg = { type: 'post', text: 'Testing ' + keys.id }
  const msg2 = { type: 'post', text: 'Testing ' + keys2.id }
  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg, Date.now())
  state = validate.appendNew(state, null, keys2, msg2, Date.now() + 1)

  const filterQuery = {
    type: 'AND',
    data: [
      {
        type: 'EQUAL',
        data: {
          seek: helpers.seekAuthor,
          value: Buffer.from(keys.id),
          indexType: 'author',
          indexName: 'author_' + keys.id,
        },
      },
      {
        type: 'GTE',
        data: {
          value: 1,
          indexName: 'sequence',
        },
      },
    ],
  }

  var i = 1
  pull(
    db.live(filterQuery),
    pull.drain((result) => {
      if (i++ == 1) {
        t.equal(result.key, state.queue[0].key)
        addMsg(state.queue[1].value, raf, (err, msg1) => {})

        setTimeout(() => {
          t.end()
        }, 500)
      } else {
        t.fail('should only be called once')
      }
    })
  )

  addMsg(state.queue[0].value, raf, (err, msg1) => {
    // console.log("waiting for live query")
  })
})

prepareAndRunTest('Live with initial values', dir, (t, db, raf) => {
  const msg = { type: 'post', text: 'Testing!' }
  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg, Date.now())
  state = validate.appendNew(state, null, keys2, msg, Date.now() + 1)

  const typeQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekType,
      value: Buffer.from('post'),
      indexType: 'type',
      indexName: 'type_post',
    },
  }

  addMsg(state.queue[0].value, raf, (err, msg1) => {
    // create index
    db.all(typeQuery, 0, false, false, (err, results) => {
      t.equal(results.length, 1)

      pull(
        db.live(typeQuery),
        pull.drain((result) => {
          t.equal(result.key, state.queue[1].key)

          // rerun on updated index
          db.all(typeQuery, 0, false, false, (err, results) => {
            t.equal(results.length, 2)
            t.end()
          })
        })
      )

      addMsg(state.queue[1].value, raf, (err, msg1) => {
        // console.log("waiting for live query")
      })
    })
  })
})

prepareAndRunTest('Live with seq values', dir, (t, db, raf) => {
  let state = validate.initial()

  const n = 1001

  let a = []
  for (var i = 0; i < n; ++i) {
    let msg = { type: 'post', text: 'Testing!' }
    msg.i = i
    if (i > 0 && i % 2 == 0) msg.type = 'non-post'
    else msg.type = 'post'
    state = validate.appendNew(state, null, keys, msg, Date.now() + i)
    if (i > 0) a.push(i)
  }

  let ps = pull(
    pull.values(a),
    pull.asyncMap((i, cb) => {
      addMsg(state.queue[i].value, raf, (err) => cb(err, i))
    })
  )

  const typeQuery = {
    type: 'AND',
    data: [
      {
        type: 'EQUAL',
        data: {
          seek: helpers.seekType,
          value: Buffer.from('post'),
          indexType: 'type',
          indexName: 'type_post',
        },
      },
      {
        type: 'OR',
        data: [
          {
            type: 'SEQS',
            seqs: [0],
          },
          {
            type: 'LIVESEQS',
            stream: ps,
          },
        ],
      },
    ],
  }

  addMsg(state.queue[0].value, raf, (err, msg1) => {
    db.all(typeQuery, 0, false, false, (err, results) => {
      t.equal(results.length, 1)

      let liveI = 1

      pull(
        db.live(typeQuery),
        pull.drain((result) => {
          if (result.key !== state.queue[liveI].key) {
            t.fail('result.key did not match state.queue[liveI].key')
          }
          liveI += 2
          if (liveI == n) t.end()
        })
      )
    })
  })
})

prepareAndRunTest('Live with cleanup', dir, (t, db, raf) => {
  const msg = { type: 'post', text: 'Testing!' }
  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg, Date.now())
  state = validate.appendNew(state, null, keys2, msg, Date.now() + 1)

  const typeQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekType,
      value: Buffer.from('post'),
      indexType: 'type',
      indexName: 'type_post',
    },
  }

  pull(
    db.live(typeQuery),
    pull.drain((result) => {
      t.equal(result.key, state.queue[0].key)

      // add second live query
      pull(
        db.live(typeQuery),
        pull.drain((result) => {
          t.equal(result.key, state.queue[1].key)
          t.end()
        })
      )

      return false // abort
    })
  )

  addMsg(state.queue[0].value, raf, (err, msg1) => {
    addMsg(state.queue[1].value, raf, (err, msg1) => {
      // console.log("waiting for live query")
    })
  })
})
