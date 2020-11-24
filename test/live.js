const test = require('tape')
const validate = require('ssb-validate')
const ssbKeys = require('ssb-keys')
const path = require('path')
const { prepareAndRunTest, addMsg, helpers } = require('./common')()
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const pull = require('pull-stream')
const Abortable = require('pull-abortable')

const dir = '/tmp/jitdb-live'
rimraf.sync(dir)
mkdirp.sync(dir)

var keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))
var keys2 = ssbKeys.loadOrCreateSync(path.join(dir, 'secret2'))
var keys3 = ssbKeys.loadOrCreateSync(path.join(dir, 'secret3'))

function drain(p, f) {
  pull(p, pull.drain(f))
}

prepareAndRunTest('Live', dir, (t, db, raf) => {
  const msg = { type: 'post', text: 'Testing!' }
  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg, Date.now())
  state = validate.appendNew(state, null, keys2, msg, Date.now())

  const typeQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekType,
      value: 'post',
      indexType: 'type',
    },
  }

  var i = 1
  db.live(typeQuery, (err, p) => {
    drain(p, (result) => {
      if (i++ == 1) {
        t.equal(result.key, state.queue[0].key)
        addMsg(state.queue[1].value, raf, (err, msg1) => {})
      } else {
        t.equal(result.key, state.queue[1].key)
        t.end()
      }
    })
  })

  addMsg(state.queue[0].value, raf, (err, msg1) => {
    // console.log("waiting for live query")
  })
})

prepareAndRunTest('Live and', dir, (t, db, raf) => {
  const msg = { type: 'post', text: 'Testing ' + keys.id }
  const msg2 = { type: 'post', text: 'Testing ' + keys2.id }
  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg, Date.now())
  state = validate.appendNew(state, null, keys2, msg2, Date.now())

  const filterQuery = {
    type: 'AND',
    data: [
      {
        type: 'EQUAL',
        data: {
          seek: helpers.seekAuthor,
          value: keys.id,
          indexType: 'author',
        },
      },
      {
        type: 'EQUAL',
        data: {
          seek: helpers.seekType,
          value: 'post',
          indexType: 'type',
        },
      },
    ],
  }

  var i = 1
  db.live(filterQuery, (err, p) => {
    drain(p, (result) => {
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
  })

  addMsg(state.queue[0].value, raf, (err, msg1) => {
    // console.log("waiting for live query")
  })
})

prepareAndRunTest('Live or', dir, (t, db, raf) => {
  const msg = { type: 'post', text: 'Testing ' + keys.id }
  const msg2 = { type: 'post', text: 'Testing ' + keys2.id }
  const msg3 = { type: 'post', text: 'Testing ' + keys3.id }
  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg, Date.now())
  state = validate.appendNew(state, null, keys2, msg2, Date.now())
  state = validate.appendNew(state, null, keys3, msg3, Date.now())

  const authorQuery = {
    type: 'OR',
    data: [
      {
        type: 'EQUAL',
        data: {
          seek: helpers.seekAuthor,
          value: keys.id,
          indexType: 'author',
        },
      },
      {
        type: 'EQUAL',
        data: {
          seek: helpers.seekAuthor,
          value: keys2.id,
          indexType: 'author',
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
          value: 'post',
          indexType: 'type',
        },
      },
    ],
  }

  var i = 1
  db.live(filterQuery, (err, p) => {
    drain(p, (result) => {
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
  })

  addMsg(state.queue[0].value, raf, (err, msg1) => {
    // console.log("waiting for live query")
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
      seek: helpers.seekType,
      value: 'post',
      indexType: 'type',
    },
  }

  addMsg(state.queue[0].value, raf, (err, msg1) => {
    // create index
    db.all(typeQuery, 0, false, (err, results) => {
      t.equal(results.length, 1)

      db.live(typeQuery, (err, p) => {
        drain(p, (result) => {
          t.equal(result.key, state.queue[1].key)

          // rerun on updated index
          db.all(typeQuery, 0, false, (err, results) => {
            t.equal(results.length, 2)
            t.end()
          })
        })
      })

      addMsg(state.queue[1].value, raf, (err, msg1) => {
        // console.log("waiting for live query")
      })
    })
  })
})

prepareAndRunTest('Live with deferred values', dir, (t, db, raf) => {
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
          value: 'post',
          indexType: 'type',
        },
      },
      {
        type: 'DEFERREDOFFSETS',
        offsets: [0],
        stream: ps,
      },
    ],
  }

  addMsg(state.queue[0].value, raf, (err, msg1) => {
    db.all(typeQuery, 0, false, (err, results) => {
      t.equal(results.length, 1)

      let deferredI = 1

      // setup deferred cb handler
      db.live(typeQuery, (err, p) => {
        console.log('draining', p)
        drain(p, (result) => {
          t.equal(result.key, state.queue[deferredI].key)
          deferredI += 2
          if (deferredI == n) t.end()
        })
      })
    })
  })
})

prepareAndRunTest('Live with cleanup', dir, (t, db, raf) => {
  const msg = { type: 'post', text: 'Testing!' }
  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg, Date.now())
  state = validate.appendNew(state, null, keys2, msg, Date.now())

  const typeQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekType,
      value: 'post',
      indexType: 'type',
    },
  }

  db.live(typeQuery, (err, p) => {
    const abortable = Abortable()

    pull(
      p,
      abortable,
      pull.drain((result) => {
        t.equal(result.key, state.queue[0].key)
        abortable.abort()

        // add second live query
        db.live(typeQuery, (err, p) => {
          drain(p, (result) => {
            t.equal(result.key, state.queue[1].key)
            t.end()
          })
        })
      })
    )
  })

  addMsg(state.queue[0].value, raf, (err, msg1) => {
    addMsg(state.queue[1].value, raf, (err, msg1) => {
      // console.log("waiting for live query")
    })
  })
})
