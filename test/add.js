// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

const test = require('tape')
const validate = require('ssb-validate')
const ssbKeys = require('ssb-keys')
const path = require('path')
const { prepareAndRunTest, addMsg, helpers } = require('./common')()
const push = require('push-stream')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const { safeFilename } = require('../files')

const dir = '/tmp/jitdb-add'
rimraf.sync(dir)
mkdirp.sync(dir)

var keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))
var keys2 = ssbKeys.loadOrCreateSync(path.join(dir, 'secret2'))
var keys3 = ssbKeys.loadOrCreateSync(path.join(dir, 'secret3'))

prepareAndRunTest('Base', dir, (t, db, raf) => {
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
    addMsg(state.queue[1].value, raf, (err, msg2) => {
      db.paginate(typeQuery, 0, 10, false, false, (err, { results }) => {
        t.equal(results.length, 2)

        // rerun on created index
        db.paginate(typeQuery, 0, 10, true, false, (err, { results }) => {
          t.equal(results.length, 2)
          t.equal(results[0].value.author, keys2.id)

          db.paginate(typeQuery, 0, 10, false, false, (err, { results }) => {
            t.equal(results.length, 2)
            t.equal(results[0].value.author, keys.id)

            const authorQuery = {
              type: 'EQUAL',
              data: {
                seek: helpers.seekAuthor,
                value: Buffer.from(keys.id),
                indexType: 'author',
                indexName: 'author_' + keys.id,
              },
            }
            db.paginate(
              authorQuery,
              0,
              10,
              false,
              false,
              (err, { results }) => {
                t.equal(results.length, 1)
                t.equal(results[0].id, msg1.id)

                // rerun on created index
                db.paginate(
                  authorQuery,
                  0,
                  10,
                  false,
                  false,
                  (err, { results }) => {
                    t.equal(results.length, 1)
                    t.equal(results[0].id, msg1.id)

                    db.paginate(
                      {
                        type: 'AND',
                        data: [authorQuery, typeQuery],
                      },
                      0,
                      10,
                      false,
                      false,
                      (err, { results }) => {
                        t.equal(results.length, 1)
                        t.equal(results[0].id, msg1.id)

                        const authorQuery2 = {
                          type: 'EQUAL',
                          data: {
                            seek: helpers.seekAuthor,
                            value: Buffer.from(keys2.id),
                            indexType: 'author',
                            indexName: 'author_' + keys2.id,
                          },
                        }

                        db.paginate(
                          {
                            type: 'AND',
                            data: [
                              typeQuery,
                              {
                                type: 'OR',
                                data: [authorQuery, authorQuery2],
                              },
                            ],
                          },
                          0,
                          10,
                          false,
                          false,
                          (err, { results }) => {
                            t.equal(results.length, 2)
                            t.end()
                          }
                        )
                      }
                    )
                  }
                )
              }
            )
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

  t.equal(typeof db.status, 'function')
  t.equal(db.status.value['seq'], -1)
  t.equal(typeof db.status.value['type_post'], 'undefined')

  addMsg(state.queue[0].value, raf, (err, msg1) => {
    db.all(typeQuery, 0, false, false, (err, results) => {
      t.equal(results.length, 1)
      t.equal(db.status.value['seq'], 0)
      t.equal(db.status.value['type_post'], 0)

      addMsg(state.queue[1].value, raf, (err, msg1) => {
        db.all(typeQuery, 0, false, false, (err, results) => {
          t.equal(results.length, 2)
          t.equal(db.status.value['seq'], 352)
          t.equal(db.status.value['type_post'], 352)
          t.end()
        })
      })
    })
  })
})

prepareAndRunTest('obsolete status parts disappear', dir, (t, db, raf) => {
  let post = { type: 'post', text: 'Testing' }

  let state = validate.initial()
  for (var i = 0; i < 10; ++i) {
    post.text = 'Testing ' + i
    state = validate.appendNew(state, null, keys, post, Date.now() + i)
  }

  const typeQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekType,
      value: Buffer.from('post'),
      indexType: 'type',
      indexName: 'type_post',
    },
  }

  push(
    push.values(state.queue),
    push.asyncMap((q, cb) => {
      addMsg(q.value, raf, cb)
    }),
    push.collect(() => {
      db.paginate(typeQuery, 0, 1, false, false, () => {
        t.pass(JSON.stringify(db.status.value))
        t.ok(db.status.value['seq'])
        t.ok(db.status.value['type_post'])
        t.notOk(db.status.value['type_about'])

        let about = { type: 'about', text: 'Testing' }
        for (var i = 0; i < 30000; ++i) {
          about.text = 'Testing ' + i
          state = validate.appendNew(state, null, keys, about, Date.now() + i)
        }
        const aboutQuery = {
          type: 'EQUAL',
          data: {
            seek: helpers.seekType,
            value: Buffer.from('about'),
            indexType: 'type',
            indexName: 'type_about',
          },
        }

        push(
          push.values(state.queue),
          push.asyncMap((q, cb) => {
            addMsg(q.value, raf, cb)
          }),
          push.collect(() => {
            db.paginate(aboutQuery, 0, 1, false, false, () => {
              t.pass(JSON.stringify(db.status.value))
              t.ok(db.status.value['seq'])
              t.notOk(db.status.value['type_post'])
              t.ok(db.status.value['type_about'])
              t.end()
            })
          })
        )
      })
    })
  )
})

prepareAndRunTest('grow', dir, (t, db, raf) => {
  let msg = { type: 'post', text: 'Testing' }

  let state = validate.initial()
  for (var i = 0; i < 32 * 1000; ++i) {
    msg.text = 'Testing ' + i
    state = validate.appendNew(state, null, keys, msg, Date.now() + i)
  }

  const typeQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekType,
      value: Buffer.from('post'),
      indexType: 'type',
      indexName: 'type_post',
    },
  }

  push(
    push.values(state.queue),
    push.asyncMap((q, cb) => {
      addMsg(q.value, raf, cb)
    }),
    push.collect((err, results) => {
      db.paginate(typeQuery, 0, 1, false, false, (err, { results }) => {
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
  state = validate.appendNew(state, null, keys, msgContact, Date.now() + 1)
  state = validate.appendNew(state, null, keys2, msg2, Date.now() + 2)
  state = validate.appendNew(state, null, keys3, msg3, Date.now() + 3)

  const authorQuery = {
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
        type: 'EQUAL',
        data: {
          seek: helpers.seekAuthor,
          value: Buffer.from(keys.id),
          indexType: 'author',
          indexAll: true,
          indexName: safeFilename('author_' + keys.id),
        },
      },
    ],
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    addMsg(state.queue[1].value, raf, (err, msg) => {
      addMsg(state.queue[2].value, raf, (err, msg) => {
        addMsg(state.queue[3].value, raf, (err, msg) => {
          db.all(authorQuery, 0, false, false, (err, results) => {
            t.error(err)
            t.equal(results.length, 1)
            t.equal(results[0].value.content.text, 'Testing 1')
            t.equal(Object.keys(db.indexes).length, 3 + 2 + 1 + 1)
            t.end()
          })
        })
      })
    })
  })
})

prepareAndRunTest('indexAll multiple reindexes', dir, (t, db, raf) => {
  const msg = { type: 'post', text: 'Testing 1' }
  const msgContact = { type: 'contact' }
  const msg2 = { type: 'post', text: 'Testing 2' }
  const msgAbout = { type: 'about' }
  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg, Date.now())
  state = validate.appendNew(state, null, keys, msgContact, Date.now() + 1)
  state = validate.appendNew(state, null, keys2, msg2, Date.now() + 2)
  state = validate.appendNew(state, null, keys3, msgAbout, Date.now() + 3)

  function typeQuery(value) {
    return {
      type: 'EQUAL',
      data: {
        seek: helpers.seekType,
        value: Buffer.from(value),
        indexType: 'type',
        indexAll: true,
        indexName: safeFilename('type_' + value),
      },
    }
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    addMsg(state.queue[1].value, raf, (err, msg) => {
      db.all(typeQuery('post'), 0, false, false, (err, results) => {
        t.equal(results.length, 1)
        t.equal(results[0].value.content.text, 'Testing 1')

        addMsg(state.queue[2].value, raf, (err, msg) => {
          addMsg(state.queue[3].value, raf, (err, msg) => {
            db.all(typeQuery('about'), 0, false, false, (err, results) => {
              t.equal(results.length, 1)

              db.all(typeQuery('post'), 0, false, false, (err, results) => {
                t.equal(results.length, 2)
                t.deepEqual(db.indexes['type_post'].bitset.array(), [0, 2])
                t.deepEqual(db.indexes['type_contact'].bitset.array(), [1])
                t.deepEqual(db.indexes['type_about'].bitset.array(), [3])
                t.end()
              })
            })
          })
        })
      })
    })
  })
})
