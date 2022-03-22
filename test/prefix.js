// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

const validate = require('ssb-validate')
const ssbKeys = require('ssb-keys')
const path = require('path')
const { prepareAndRunTest, addMsg, helpers } = require('./common')()
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')

const jitdb = require('../index')

const dir = '/tmp/jitdb-prefix'
rimraf.sync(dir)
mkdirp.sync(dir)

var keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

prepareAndRunTest('Prefix equal', dir, (t, db, raf) => {
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
      value: helpers.toBipf('post'),
      indexType: 'type',
      indexName: 'value_content_type_post',
      prefix: 32,
    },
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    addMsg(state.queue[1].value, raf, (err, msg) => {
      addMsg(state.queue[2].value, raf, (err, msg) => {
        db.all(typeQuery, 0, false, false, 'declared', null, (err, results) => {
          t.equal(results.length, 2)
          t.equal(results[0].value.content.type, 'post')
          t.equal(results[1].value.content.type, 'post')
          t.end()
        })
      })
    })
  })
})

prepareAndRunTest('Normal index renamed to prefix', dir, (t, db, raf) => {
  const msg1 = { type: 'post', text: 'Testing!' }
  const msg2 = { type: 'contact', text: 'Testing!' }
  const msg3 = { type: 'post', text: 'Testing 2!' }

  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg1, Date.now())
  state = validate.appendNew(state, null, keys, msg2, Date.now() + 1)
  state = validate.appendNew(state, null, keys, msg3, Date.now() + 2)

  const normalQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekType,
      value: helpers.toBipf('post'),
      indexType: 'type',
      indexName: 'value_content_type_post',
    },
  }

  const prefixQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekType,
      value: helpers.toBipf('post'),
      indexType: 'type',
      indexName: 'value_content_type',
      prefix: 32,
    },
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    addMsg(state.queue[1].value, raf, (err, msg) => {
      addMsg(state.queue[2].value, raf, (err, msg) => {
        db.all(
          normalQuery,
          0,
          false,
          false,
          'declared',
          null,
          (err, results) => {
            t.equal(results.length, 2)
            t.equal(results[0].value.content.type, 'post')
            t.equal(results[1].value.content.type, 'post')
            db.all(
              prefixQuery,
              0,
              false,
              false,
              'declared',
              null,
              (err, results2) => {
                t.equal(results2.length, 2)
                t.equal(results2[0].value.content.type, 'post')
                t.equal(results2[1].value.content.type, 'post')
                t.end()
              }
            )
          }
        )
      })
    })
  })
})

prepareAndRunTest('Prefix index skips deleted records', dir, (t, db, raf) => {
  const msg1 = { type: 'post', text: 'Testing!' }
  const msg2 = { type: 'contact', text: 'Testing!' }
  const msg3 = { type: 'post', text: 'Testing 2!' }

  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg1, Date.now())
  state = validate.appendNew(state, null, keys, msg2, Date.now() + 1)
  state = validate.appendNew(state, null, keys, msg3, Date.now() + 2)

  const prefixQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekType,
      value: helpers.toBipf('post'),
      indexType: 'type',
      indexName: 'value_content_type_post',
      prefix: 32,
    },
  }

  // Cloned to avoid the op-to-bitset cache
  const prefixQuery2 = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekType,
      value: helpers.toBipf('post'),
      indexType: 'type',
      indexName: 'value_content_type_post',
      prefix: 32,
    },
  }

  addMsg(state.queue[0].value, raf, (err1) => {
    addMsg(state.queue[1].value, raf, (err2) => {
      addMsg(state.queue[2].value, raf, (err3) => {
        db.all(
          prefixQuery,
          0,
          false,
          true,
          'declared',
          null,
          (err4, offsets) => {
            t.error(err4, 'no err4')
            t.deepEqual(offsets, [0, 760])
            raf.del(760, (err5) => {
              t.error(err5, 'no err5')
              db.all(
                prefixQuery2,
                0,
                false,
                false,
                'declared',
                null,
                (err6, results) => {
                  t.error(err6, 'no err6')
                  t.equal(results.length, 1)
                  t.equal(results[0].value.content.type, 'post')
                  t.equal(results[0].value.content.text, 'Testing!')
                  t.end()
                }
              )
            })
          }
        )
      })
    })
  })
})

prepareAndRunTest('Prefix larger than actual value', dir, (t, db, raf) => {
  const msg1 = { type: 'post', text: 'First', channel: 'foo' }
  const msg2 = { type: 'contact', text: 'Second' }
  const msg3 = { type: 'post', text: 'Third' }

  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg1, Date.now())
  state = validate.appendNew(state, null, keys, msg2, Date.now() + 1)
  state = validate.appendNew(state, null, keys, msg3, Date.now() + 2)

  const channelQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekChannel,
      value: helpers.toBipf('foo'),
      indexType: 'channel',
      indexName: 'value_content_channel_foo',
      prefix: 32,
    },
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    addMsg(state.queue[1].value, raf, (err, msg) => {
      addMsg(state.queue[2].value, raf, (err, msg) => {
        db.all(
          channelQuery,
          0,
          false,
          false,
          'declared',
          null,
          (err, results) => {
            t.equal(results.length, 1)
            t.equal(results[0].value.content.text, 'First')
            t.end()
          }
        )
      })
    })
  })
})

prepareAndRunTest('Prefix equal falsy', dir, (t, db, raf) => {
  const msg1 = { type: 'post', text: 'First', channel: 'foo' }
  const msg2 = { type: 'contact', text: 'Second' }
  const msg3 = { type: 'post', text: 'Third' }

  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg1, Date.now())
  state = validate.appendNew(state, null, keys, msg2, Date.now() + 1)
  state = validate.appendNew(state, null, keys, msg3, Date.now() + 2)

  const channelQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekChannel,
      value: null,
      indexType: 'channel',
      indexName: 'value_content_channel_',
      prefix: 32,
    },
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    addMsg(state.queue[1].value, raf, (err, msg) => {
      addMsg(state.queue[2].value, raf, (err, msg) => {
        db.all(
          channelQuery,
          0,
          false,
          false,
          'declared',
          null,
          (err, results) => {
            t.equal(results.length, 2)
            t.equal(results[0].value.content.text, 'Second')
            t.equal(results[1].value.content.text, 'Third')
            t.end()
          }
        )
      })
    })
  })
})

prepareAndRunTest('Prefix equal', dir, (t, db, raf) => {
  const name = 'Prefix equal'

  const msg1 = { type: 'post', text: 'Testing!' }
  const msg2 = { type: 'contact', text: 'Testing!' }
  const msg3 = {
    type: 'vote',
    vote: {
      link: '%wOtfXXopI3mTHL6F7Y3XXNtpxws9mQdaEocNJuKtAZo=.sha256',
      value: 1,
      expression: 'Like',
    },
  }
  const msg4 = {
    type: 'vote',
    vote: {
      link: '%wOtfXXopI3mTHL6F7Y3XXNtpxws9mQdaEocNJuKtAZo=.sha256',
      value: 0,
      expression: 'UnLike',
    },
  }
  const msg5 = {
    type: 'vote',
    vote: {
      link: '%wOtfXXopI3mTHL6F7Y3XXNtpxws9mQdaEocNJuKtAZo=.sha256',
      value: 1,
      expression: 'Like',
    },
  }

  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg1, Date.now())
  state = validate.appendNew(state, null, keys, msg2, Date.now() + 1)
  state = validate.appendNew(state, null, keys, msg3, Date.now() + 2)
  state = validate.appendNew(state, null, keys, msg4, Date.now() + 3)
  state = validate.appendNew(state, null, keys, msg5, Date.now() + 4)

  const voteQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekVoteLink,
      value: helpers.toBipf(
        '%wOtfXXopI3mTHL6F7Y3XXNtpxws9mQdaEocNJuKtAZo=.sha256'
      ),
      indexType: 'vote',
      indexName:
        'value_content_vote_link_%wOtfXXopI3mTHL6F7Y3XXNtpxws9mQdaEocNJuKtAZo=.sha256',
      prefix: 32,
    },
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    addMsg(state.queue[1].value, raf, (err, msg) => {
      addMsg(state.queue[2].value, raf, (err, msg) => {
        db.all(voteQuery, 0, false, false, 'declared', null, (err, results) => {
          t.equal(results.length, 1)
          t.equal(results[0].value.content.type, 'vote')

          db = jitdb(raf, path.join(dir, 'indexes' + name))
          db.onReady(() => {
            addMsg(state.queue[3].value, raf, (err, msg) => {
              db.all(
                voteQuery,
                0,
                false,
                false,
                'declared',
                null,
                (err, results) => {
                  t.equal(results.length, 2)

                  db = jitdb(raf, path.join(dir, 'indexes' + name))
                  db.onReady(() => {
                    addMsg(state.queue[4].value, raf, (err, msg) => {
                      db.all(
                        voteQuery,
                        0,
                        false,
                        false,
                        'declared',
                        null,
                        (err, results) => {
                          t.equal(results.length, 3)
                          t.equal(results[0].value.content.type, 'vote')
                          t.equal(results[1].value.content.type, 'vote')
                          t.equal(results[2].value.content.type, 'vote')
                          t.end()
                        }
                      )
                    })
                  })
                }
              )
            })
          })
        })
      })
    })
  })
})

prepareAndRunTest('Prefix equal unknown value', dir, (t, db, raf) => {
  const msg1 = { type: 'post', text: 'First', channel: 'foo' }
  const msg2 = { type: 'contact', text: 'Second' }
  const msg3 = { type: 'post', text: 'Third' }

  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg1, Date.now())
  state = validate.appendNew(state, null, keys, msg2, Date.now() + 1)
  state = validate.appendNew(state, null, keys, msg3, Date.now() + 2)

  const authorQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekAuthor,
      value: helpers.toBipf('abc'),
      indexType: 'author',
      indexName: 'value_author_abc',
      prefix: 32,
    },
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    addMsg(state.queue[1].value, raf, (err, msg) => {
      addMsg(state.queue[2].value, raf, (err, msg) => {
        db.all(
          authorQuery,
          0,
          false,
          false,
          'declared',
          null,
          (err, results) => {
            t.equal(results.length, 0)
            t.end()
          }
        )
      })
    })
  })
})

prepareAndRunTest('Prefix map equal', dir, (t, db, raf) => {
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
      value: helpers.toBipf('post'),
      indexType: 'type',
      indexName: 'value_content_type_post',
      useMap: true,
      prefix: 32,
    },
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    addMsg(state.queue[1].value, raf, (err, msg) => {
      addMsg(state.queue[2].value, raf, (err, msg) => {
        db.all(typeQuery, 0, false, false, 'declared', null, (err, results) => {
          t.equal(results.length, 2)
          t.equal(results[0].value.content.type, 'post')
          t.equal(results[1].value.content.type, 'post')
          t.end()
        })
      })
    })
  })
})

prepareAndRunTest('Prefix offset', dir, (t, db, raf) => {
  const msg1 = { type: 'post', text: 'Testing!' }
  const msg2 = { type: 'contact', text: 'Testing!' }
  const msg3 = { type: 'post', text: 'Testing 2!' }

  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg1, Date.now())
  state = validate.appendNew(state, null, keys, msg2, Date.now() + 1)
  state = validate.appendNew(state, null, keys, msg3, Date.now() + 2)

  addMsg(state.queue[0].value, raf, (err, msg) => {
    addMsg(state.queue[1].value, raf, (err, msg) => {
      addMsg(state.queue[2].value, raf, (err, msg) => {
        const keyQuery = {
          type: 'EQUAL',
          data: {
            seek: helpers.seekKey,
            value: helpers.toBipf(msg.key),
            indexType: 'key',
            indexName: 'value_key_' + msg.key,
            useMap: true,
            prefix: 32,
            prefixOffset: 1,
          },
        }

        db.all(keyQuery, 0, false, false, 'declared', null, (err, results) => {
          t.equal(results.length, 1)
          t.equal(results[0].value.content.text, 'Testing 2!')
          t.end()
        })
      })
    })
  })
})

prepareAndRunTest('Prefix offset 1 on empty', dir, (t, db, raf) => {
  const msg1 = { type: 'post', root: 'test', text: 'Testing!' }

  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg1, Date.now())

  addMsg(state.queue[0].value, raf, (err, msg) => {
    const rootQuery = {
      type: 'EQUAL',
      data: {
        seek: helpers.seekRoot,
        value: helpers.toBipf('test'),
        indexType: 'root',
        indexName: 'value_content_root',
        useMap: true,
        prefix: 32,
        prefixOffset: 1,
      },
    }

    db.all(rootQuery, 0, false, false, 'declared', null, (err, results) => {
      t.equal(results.length, 1)
      t.equal(results[0].value.content.text, 'Testing!')
      t.end()
    })
  })
})

prepareAndRunTest('Prefix delete', dir, (t, db, raf) => {
  const msg1 = { type: 'post', text: 'Testing!' }
  const msg2 = { type: 'contact', text: 'Contact!' }
  const msg3 = { type: 'post', text: 'Testing 2!' }

  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg1, Date.now())
  state = validate.appendNew(state, null, keys, msg2, Date.now() + 1)
  state = validate.appendNew(state, null, keys, msg3, Date.now() + 2)

  const typeQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekType,
      value: helpers.toBipf('post'),
      indexType: 'type',
      indexName: 'value_content_type_post',
      useMap: true,
      prefix: 32,
    },
  }
  const authorQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekAuthor,
      value: helpers.toBipf(keys.id),
      indexType: 'author',
      indexName: 'value_author',
      prefix: 32,
    },
  }

  addMsg(state.queue[0].value, raf, (err, msg, offset1) => {
    addMsg(state.queue[1].value, raf, (err, msg) => {
      addMsg(state.queue[2].value, raf, (err, msg) => {
        db.all(
          {
            type: 'AND',
            data: [typeQuery, authorQuery],
          },
          0,
          false,
          false,
          'declared',
          null,
          (err, results) => {
            t.equal(results.length, 2)
            t.equal(results[0].value.content.type, 'post')
            t.equal(results[1].value.content.type, 'post')

            raf.del(offset1, () => {
              db.paginate(
                {
                  type: 'AND',
                  data: [typeQuery, authorQuery],
                },
                0,
                1,
                false,
                false,
                'declared',
                null,
                (err, answer) => {
                  t.equal(answer.results.length, 1)
                  t.equal(answer.results[0].value.content.text, 'Testing 2!')

                  t.end()
                }
              )
            })
          }
        )
      })
    })
  })
})
