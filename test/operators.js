const test = require('tape')
const pull = require('pull-stream')
const Pushable = require('pull-pushable')
const validate = require('ssb-validate')
const ssbKeys = require('ssb-keys')
const { prepareAndRunTest, addMsg, helpers } = require('./common')()
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const {
  query,
  and,
  or,
  equal,
  slowEqual,
  includes,
  slowIncludes,
  gt,
  gte,
  lt,
  lte,
  deferred,
  liveOffsets,
  offsets,
  seqs,
  fromDB,
  paginate,
  startFrom,
  live,
  descending,
  toCallback,
  toPromise,
  toPullStream,
  toAsyncIter,
} = require('../operators')

const dir = '/tmp/jitdb-query-api'
rimraf.sync(dir)
mkdirp.sync(dir)

const alice = ssbKeys.generate('ed25519', Buffer.alloc(32, 'a'))
const bob = ssbKeys.generate('ed25519', Buffer.alloc(32, 'b'))

prepareAndRunTest('operators API supports equal', dir, (t, db, raf) => {
  const queryTree = query(
    fromDB(db),
    and(equal(helpers.seekType, 'post', { indexType: 'type', indexAll: true }))
  )

  t.equal(typeof queryTree, 'object', 'queryTree is an object')

  t.equal(queryTree.type, 'EQUAL')

  t.equal(queryTree.data.indexType, 'type')
  t.equal(queryTree.data.indexAll, true)
  t.deepEqual(queryTree.data.value, Buffer.from('post'))
  t.true(queryTree.data.seek.toString().includes('bipf.seekKey'))

  t.equal(typeof queryTree.meta, 'object', 'queryTree contains meta')
  t.equal(typeof queryTree.meta.db, 'object', 'queryTree contains meta.db')
  t.equal(typeof queryTree.meta.db.onReady, 'function', 'meta.db looks correct')

  t.end()
})

prepareAndRunTest('operators API supports slowEqual', dir, (t, db, raf) => {
  const queryTree = query(
    fromDB(db),
    and(slowEqual('value.content.type', 'post'))
  )

  t.equal(typeof queryTree, 'object', 'queryTree is an object')

  t.equal(queryTree.type, 'EQUAL')

  t.equal(queryTree.data.indexType, 'value_content_type')
  t.notOk(queryTree.data.indexAll)
  t.deepEqual(queryTree.data.value, Buffer.from('post'))
  t.true(queryTree.data.seek.toString().includes('bipf.seekKey'))

  t.equal(typeof queryTree.meta, 'object', 'queryTree contains meta')
  t.equal(typeof queryTree.meta.db, 'object', 'queryTree contains meta.db')
  t.equal(typeof queryTree.meta.db.onReady, 'function', 'meta.db looks correct')

  t.end()
})

prepareAndRunTest('slowEqual 3 args', dir, (t, db, raf) => {
  const queryTree = slowEqual('value.content.type', 'post', { indexAll: true })

  t.equal(typeof queryTree, 'object', 'queryTree is an object')

  t.equal(queryTree.type, 'EQUAL')

  t.equal(queryTree.data.indexType, 'value_content_type')
  t.equal(queryTree.data.indexAll, true)
  t.deepEqual(queryTree.data.value, Buffer.from('post'))
  t.true(queryTree.data.seek.toString().includes('bipf.seekKey'))

  t.end()
})

prepareAndRunTest('equal with null value', dir, (t, db, raf) => {
  const queryTree = equal(helpers.seekChannel, null, {
    indexType: 'channel',
  })

  t.equal(typeof queryTree, 'object', 'queryTree is an object')

  t.equal(queryTree.type, 'EQUAL')

  t.equal(queryTree.data.indexType, 'channel')
  t.notOk(queryTree.data.value)
  t.true(queryTree.data.seek.toString().includes('bipf.seekKey'))

  t.end()
})

prepareAndRunTest('equal with undefined value', dir, (t, db, raf) => {
  const queryTree = equal(helpers.seekChannel, undefined, {
    indexType: 'channel',
  })

  t.equal(typeof queryTree, 'object', 'queryTree is an object')

  t.equal(queryTree.type, 'EQUAL')

  t.equal(queryTree.data.indexType, 'channel')
  t.notOk(queryTree.data.value)
  t.true(queryTree.data.seek.toString().includes('bipf.seekKey'))

  t.end()
})

prepareAndRunTest('equal with prefix', dir, (t, db, raf) => {
  const queryTree = equal(helpers.seekType, 'post', {
    prefix: 32,
    indexType: 'type',
  })

  t.equal(typeof queryTree, 'object', 'queryTree is an object')

  t.equal(queryTree.type, 'EQUAL')

  t.equal(queryTree.data.indexType, 'type')
  t.deepEqual(queryTree.data.value, Buffer.from('post'))
  t.true(queryTree.data.seek.toString().includes('bipf.seekKey'))
  t.equal(queryTree.data.prefix, 32)

  t.end()
})

prepareAndRunTest('slowEqual with prefix', dir, (t, db, raf) => {
  const queryTree = slowEqual('value.content.type', 'post', {
    prefix: 32,
    indexType: 'type',
  })

  t.equal(typeof queryTree, 'object', 'queryTree is an object')

  t.equal(queryTree.type, 'EQUAL')

  t.equal(queryTree.data.indexType, 'value_content_type')
  t.deepEqual(queryTree.data.value, Buffer.from('post'))
  t.true(queryTree.data.seek.toString().includes('bipf.seekKey'))
  t.equal(queryTree.data.prefix, 32)

  t.end()
})

prepareAndRunTest('includes()', dir, (t, db, raf) => {
  const queryTree = includes(helpers.seekAnimals, 'cat', {
    indexType: 'animals',
  })

  t.equal(typeof queryTree, 'object', 'queryTree is an object')

  t.equal(queryTree.type, 'INCLUDES')

  t.equal(queryTree.data.indexType, 'animals')
  t.deepEqual(queryTree.data.value, Buffer.from('cat'))
  t.true(queryTree.data.seek.toString().includes('bipf.seekKey'))

  t.end()
})

prepareAndRunTest('operators API supports and or', dir, (t, db, raf) => {
  const queryTree = query(
    fromDB(db),
    and(slowEqual('value.content.type', 'post')),
    and(
      or(slowEqual('value.author', alice.id), slowEqual('value.author', bob.id))
    )
  )

  t.equal(typeof queryTree, 'object', 'queryTree is an object')

  t.equal(queryTree.type, 'AND')
  t.true(Array.isArray(queryTree.data), '.data is an array')

  t.equal(queryTree.data[0].type, 'EQUAL')
  t.equal(queryTree.data[0].data.indexType, 'value_content_type')
  t.deepEqual(queryTree.data[0].data.value, Buffer.from('post'))

  t.equal(queryTree.data[1].type, 'OR')
  t.true(Array.isArray(queryTree.data[1].data), '.data[1].data is an array')

  t.equal(queryTree.data[1].data[0].type, 'EQUAL')
  t.deepEqual(queryTree.data[1].data[0].data.indexType, 'value_author')
  t.deepEqual(queryTree.data[1].data[0].data.value, Buffer.from(alice.id))
  t.true(
    queryTree.data[1].data[0].data.seek.toString().includes('bipf.seekKey')
  )

  t.equal(queryTree.data[1].data[1].type, 'EQUAL')
  t.equal(queryTree.data[1].data[1].data.indexType, 'value_author')
  t.deepEqual(queryTree.data[1].data[1].data.value, Buffer.from(bob.id))
  t.true(
    queryTree.data[1].data[1].data.seek.toString().includes('bipf.seekKey')
  )

  t.end()
})

prepareAndRunTest('operators multi and', dir, (t, db, raf) => {
  const queryTree = query(
    fromDB(db),
    and(
      slowEqual('value.content.type', 'post'),
      slowEqual('value.author', alice.id),
      slowEqual('value.author', bob.id)
    )
  )

  t.equal(typeof queryTree, 'object', 'queryTree is an object')

  t.equal(queryTree.type, 'AND')
  t.true(Array.isArray(queryTree.data), '.data is an array')

  t.equal(queryTree.data[0].type, 'EQUAL')
  t.equal(queryTree.data[1].type, 'EQUAL')
  t.equal(queryTree.data[2].type, 'EQUAL')

  t.end()
})

prepareAndRunTest('operators multi or', dir, (t, db, raf) => {
  const queryTree = query(
    fromDB(db),
    or(
      slowEqual('value.content.type', 'post'),
      slowEqual('value.author', alice.id),
      slowEqual('value.author', bob.id)
    )
  )

  t.equal(typeof queryTree, 'object', 'queryTree is an object')

  t.equal(queryTree.type, 'OR')
  t.true(Array.isArray(queryTree.data), '.data is an array')

  t.equal(queryTree.data[0].type, 'EQUAL')
  t.equal(queryTree.data[1].type, 'EQUAL')
  t.equal(queryTree.data[2].type, 'EQUAL')

  t.end()
})

prepareAndRunTest(
  'operators paginate startFrom descending',
  dir,
  (t, db, raf) => {
    const queryTreePaginate = query(
      fromDB(db),
      and(slowEqual('value.content.type', 'post')),
      paginate(10)
    )

    const queryTreeStartFrom = query(
      fromDB(db),
      and(slowEqual('value.content.type', 'post')),
      startFrom(5)
    )

    const queryTreeDescending = query(
      fromDB(db),
      and(slowEqual('value.content.type', 'post')),
      descending()
    )

    const queryTreeAll = query(
      fromDB(db),
      and(slowEqual('value.content.type', 'post')),
      startFrom(5),
      paginate(10),
      descending()
    )

    t.equal(queryTreePaginate.meta.pageSize, 10)
    t.equal(queryTreeStartFrom.meta.offset, 5)
    t.equal(queryTreeDescending.meta.descending, true)

    t.equal(queryTreeAll.meta.pageSize, 10)
    t.equal(queryTreeAll.meta.offset, 5)
    t.equal(queryTreeAll.meta.descending, true)

    t.end()
  }
)

prepareAndRunTest('operator gt', dir, (t, db, raf) => {
  const queryTree = query(
    fromDB(db),
    and(
      equal(helpers.seekAuthor, alice.id, { indexType: 'author' }),
      gt(2, 'sequence')
    )
  )

  t.equal(typeof queryTree, 'object', 'queryTree is an object')

  t.equal(queryTree.type, 'AND')
  t.true(Array.isArray(queryTree.data), '.data is an array')

  t.equal(queryTree.data[0].type, 'EQUAL')
  t.equal(queryTree.data[0].data.indexType, 'author')
  t.deepEqual(queryTree.data[0].data.value, Buffer.from(alice.id))
  t.true(queryTree.data[0].data.seek.toString().includes('bipf.seekKey'))

  t.equal(queryTree.data[1].type, 'GT')
  t.equal(queryTree.data[1].data.indexName, 'sequence')
  t.equal(queryTree.data[1].data.value, 2)

  t.end()
})

prepareAndRunTest('operator gte', dir, (t, db, raf) => {
  const queryTree = query(
    fromDB(db),
    and(
      equal(helpers.seekAuthor, alice.id, { indexType: 'author' }),
      gte(2, 'sequence')
    )
  )

  t.equal(typeof queryTree, 'object', 'queryTree is an object')

  t.equal(queryTree.type, 'AND')
  t.true(Array.isArray(queryTree.data), '.data is an array')

  t.equal(queryTree.data[0].type, 'EQUAL')

  t.equal(queryTree.data[1].type, 'GTE')
  t.equal(queryTree.data[1].data.indexName, 'sequence')
  t.equal(queryTree.data[1].data.value, 2)

  t.end()
})

prepareAndRunTest('operator lt', dir, (t, db, raf) => {
  const queryTree = query(
    fromDB(db),
    and(
      equal(helpers.seekAuthor, alice.id, { indexType: 'author' }),
      lt(2, 'sequence')
    )
  )

  t.equal(typeof queryTree, 'object', 'queryTree is an object')

  t.equal(queryTree.type, 'AND')
  t.true(Array.isArray(queryTree.data), '.data is an array')

  t.equal(queryTree.data[0].type, 'EQUAL')

  t.equal(queryTree.data[1].type, 'LT')
  t.equal(queryTree.data[1].data.indexName, 'sequence')
  t.equal(queryTree.data[1].data.value, 2)

  t.end()
})

prepareAndRunTest('operator lte', dir, (t, db, raf) => {
  const queryTree = query(
    fromDB(db),
    and(
      equal(helpers.seekAuthor, alice.id, { indexType: 'author' }),
      lte(2, 'sequence')
    )
  )

  t.equal(typeof queryTree, 'object', 'queryTree is an object')

  t.equal(queryTree.type, 'AND')
  t.true(Array.isArray(queryTree.data), '.data is an array')

  t.equal(queryTree.data[0].type, 'EQUAL')

  t.equal(queryTree.data[1].type, 'LTE')
  t.equal(queryTree.data[1].data.indexName, 'sequence')
  t.equal(queryTree.data[1].data.value, 2)

  t.end()
})

prepareAndRunTest('operators gt lt gte lte numbers only', dir, (t, db, raf) => {
  t.throws(() => {
    gt('2', 'sequence')
  })
  t.throws(() => {
    lt('2', 'sequence')
  })
  t.throws(() => {
    gte('2', 'sequence')
  })
  t.throws(() => {
    lte('2', 'sequence')
  })

  t.end()
})

prepareAndRunTest('operator offsets', dir, (t, db, raf) => {
  const queryTree = query(fromDB(db), and(offsets([10, 20])))

  t.equal(typeof queryTree, 'object', 'queryTree is an object')

  t.equal(queryTree.type, 'OFFSETS')
  t.true(Array.isArray(queryTree.offsets), '.offsets is an array')

  t.equal(queryTree.offsets[0], 10)
  t.equal(queryTree.offsets[1], 20)

  t.end()
})

prepareAndRunTest('operator seqs', dir, (t, db, raf) => {
  const queryTree = query(fromDB(db), and(seqs([11, 12])))

  t.equal(typeof queryTree, 'object', 'queryTree is an object')

  t.equal(queryTree.type, 'SEQS')
  t.true(Array.isArray(queryTree.seqs), '.seqs is an array')

  t.equal(queryTree.seqs[0], 11)
  t.equal(queryTree.seqs[1], 12)

  t.end()
})

prepareAndRunTest('operators fromDB then toCallback', dir, (t, db, raf) => {
  const msg = { type: 'post', text: 'Testing!' }
  let state = validate.initial()
  state = validate.appendNew(state, null, alice, msg, Date.now())
  state = validate.appendNew(state, null, bob, msg, Date.now() + 1)

  addMsg(state.queue[0].value, raf, (e1, msg1) => {
    addMsg(state.queue[1].value, raf, (e2, msg2) => {
      query(
        fromDB(db),
        toCallback((err, msgs) => {
          t.error(err, 'toCallback got no error')
          t.equal(msgs.length, 2, 'toCallback got two messages')
          t.equal(msgs[0].value.author, alice.id)
          t.equal(msgs[0].value.content.type, 'post')
          t.equal(msgs[1].value.author, bob.id)
          t.equal(msgs[1].value.content.type, 'post')
          t.end()
        })
      )
    })
  })
})

prepareAndRunTest('operators toCallback', dir, (t, db, raf) => {
  const msg = { type: 'post', text: 'Testing!' }
  let state = validate.initial()
  state = validate.appendNew(state, null, alice, msg, Date.now())
  state = validate.appendNew(state, null, bob, msg, Date.now() + 1)

  addMsg(state.queue[0].value, raf, (e1, msg1) => {
    addMsg(state.queue[1].value, raf, (e2, msg2) => {
      query(
        fromDB(db),
        or(
          slowEqual('value.author', alice.id),
          slowEqual('value.author', bob.id)
        ),
        toCallback((err, msgs) => {
          t.error(err, 'toCallback got no error')
          t.equal(msgs.length, 2, 'toCallback got two messages')
          t.equal(msgs[0].value.author, alice.id)
          t.equal(msgs[0].value.content.type, 'post')
          t.equal(msgs[1].value.author, bob.id)
          t.equal(msgs[1].value.content.type, 'post')
          t.end()
        })
      )
    })
  })
})

prepareAndRunTest('operators toPromise', dir, (t, db, raf) => {
  const msg = { type: 'post', text: 'Testing!' }
  let state = validate.initial()
  state = validate.appendNew(state, null, alice, msg, Date.now())
  state = validate.appendNew(state, null, bob, msg, Date.now() + 1)

  addMsg(state.queue[0].value, raf, (e1, msg1) => {
    addMsg(state.queue[1].value, raf, (e2, msg2) => {
      query(
        fromDB(db),
        or(
          slowEqual('value.author', alice.id),
          slowEqual('value.author', bob.id)
        ),
        toPromise()
      ).then(
        (msgs) => {
          t.equal(msgs.length, 2, 'toPromise got two messages')
          t.equal(msgs[0].value.author, alice.id)
          t.equal(msgs[0].value.content.type, 'post')
          t.equal(msgs[1].value.author, bob.id)
          t.equal(msgs[1].value.content.type, 'post')
          t.end()
        },
        (err) => {
          t.fail(err)
        }
      )
    })
  })
})

prepareAndRunTest('operators toPullStream', dir, (t, db, raf) => {
  const msg = { type: 'post', text: 'Testing!' }
  let state = validate.initial()
  state = validate.appendNew(state, null, alice, msg, Date.now())
  state = validate.appendNew(state, null, bob, msg, Date.now() + 1)

  addMsg(state.queue[0].value, raf, (e1, msg1) => {
    addMsg(state.queue[1].value, raf, (e2, msg2) => {
      pull(
        query(
          fromDB(db),
          or(
            slowEqual('value.author', alice.id),
            slowEqual('value.author', bob.id)
          ),
          paginate(2),
          toPullStream()
        ),
        pull.collect((err, pages) => {
          t.error(err, 'toPullStream got no error')
          t.equal(pages.length, 1, 'toPullStream got one page')
          const msgs = pages[0]
          t.equal(msgs.length, 2, 'page has two messages')
          t.equal(msgs[0].value.author, alice.id)
          t.equal(msgs[0].value.content.type, 'post')
          t.equal(msgs[1].value.author, bob.id)
          t.equal(msgs[1].value.content.type, 'post')
          t.end()
        })
      )
    })
  })
})

prepareAndRunTest('operators toAsyncIter', dir, (t, db, raf) => {
  const msg = { type: 'post', text: 'Testing!' }
  let state = validate.initial()
  state = validate.appendNew(state, null, alice, msg, Date.now())
  state = validate.appendNew(state, null, bob, msg, Date.now() + 1)

  addMsg(state.queue[0].value, raf, (e1, msg1) => {
    addMsg(state.queue[1].value, raf, async (e2, msg2) => {
      try {
        let i = 0
        const results = query(
          fromDB(db),
          or(
            slowEqual('value.author', alice.id),
            slowEqual('value.author', bob.id)
          ),
          paginate(2),
          toAsyncIter()
        )
        for await (let page of results) {
          t.equal(i, 0, 'just one page')
          i += 1
          const msgs = page
          t.equal(msgs.length, 2, 'page has two messages')
          t.equal(msgs[0].value.author, alice.id)
          t.equal(msgs[0].value.content.type, 'post')
          t.equal(msgs[1].value.author, bob.id)
          t.equal(msgs[1].value.content.type, 'post')
          t.end()
        }
      } catch (err) {
        t.fail(err)
      }
    })
  })
})

prepareAndRunTest('operators toCallback with startFrom', dir, (t, db, raf) => {
  const msg = { type: 'post', text: 'Testing!' }
  let state = validate.initial()
  state = validate.appendNew(state, null, alice, msg, Date.now())
  state = validate.appendNew(state, null, bob, msg, Date.now() + 1)

  addMsg(state.queue[0].value, raf, (e1, msg1) => {
    addMsg(state.queue[1].value, raf, (e2, msg2) => {
      query(
        fromDB(db),
        or(
          slowEqual('value.author', alice.id),
          slowEqual('value.author', bob.id)
        ),
        startFrom(1),
        toCallback((err, msgs) => {
          t.error(err, 'toCallback got no error')
          t.equal(msgs.length, 1, 'toCallback got one messages')
          t.equal(msgs[0].value.author, bob.id)
          t.equal(msgs[0].value.content.type, 'post')
          t.end()
        })
      )
    })
  })
})

prepareAndRunTest('operators toCallback with descending', dir, (t, db, raf) => {
  const msg = { type: 'post', text: 'Testing!' }
  let state = validate.initial()
  state = validate.appendNew(state, null, alice, msg, Date.now())
  state = validate.appendNew(state, null, bob, msg, Date.now() + 1)

  addMsg(state.queue[0].value, raf, (e1, msg1) => {
    addMsg(state.queue[1].value, raf, (e2, msg2) => {
      query(
        fromDB(db),
        or(
          slowEqual('value.author', alice.id),
          slowEqual('value.author', bob.id)
        ),
        descending(),
        toCallback((err, msgs) => {
          t.error(err, 'toCallback got no error')
          t.equal(msgs.length, 2, 'toCallback got two messages')
          t.equal(msgs[0].value.author, bob.id)
          t.equal(msgs[0].value.content.type, 'post')
          t.equal(msgs[1].value.author, alice.id)
          t.equal(msgs[1].value.content.type, 'post')
          t.end()
        })
      )
    })
  })
})

prepareAndRunTest('support deferred operations', dir, (t, db, raf) => {
  const msg = { type: 'post', text: 'Testing!' }
  let state = validate.initial()
  state = validate.appendNew(state, null, alice, msg, Date.now())
  state = validate.appendNew(state, null, bob, msg, Date.now() + 1)

  addMsg(state.queue[0].value, raf, (e1, msg1) => {
    addMsg(state.queue[1].value, raf, (e2, msg2) => {
      query(
        fromDB(db),
        and(
          deferred((meta, cb) => {
            setTimeout(() => {
              cb(null, slowEqual('value.author', alice.id))
            }, 100)
          })
        ),
        toCallback((err, msgs) => {
          t.error(err, 'toCallback got no error')
          t.equal(msgs.length, 1, 'toCallback got one message')
          t.equal(msgs[0].value.author, alice.id)
          t.equal(msgs[0].value.content.type, 'post')
          t.end()
        })
      )
    })
  })
})

prepareAndRunTest('support deferred operations and', dir, (t, db, raf) => {
  const msg = { type: 'post', text: 'Testing!' }
  let state = validate.initial()
  state = validate.appendNew(state, null, alice, msg, Date.now())
  state = validate.appendNew(state, null, bob, msg, Date.now() + 1)

  addMsg(state.queue[0].value, raf, (e1, msg1) => {
    addMsg(state.queue[1].value, raf, (e2, msg2) => {
      query(
        fromDB(db),
        and(
          slowEqual('value.content.type', 'post'),
          deferred((meta, cb) => {
            setTimeout(() => {
              cb(null, slowEqual('value.author', alice.id))
            }, 100)
          })
        ),
        toCallback((err, msgs) => {
          t.error(err, 'toCallback got no error')
          t.equal(msgs.length, 1, 'toCallback got one message')
          t.equal(msgs[0].value.author, alice.id)
          t.equal(msgs[0].value.content.type, 'post')
          t.end()
        })
      )
    })
  })
})

prepareAndRunTest('support deferred operations or', dir, (t, db, raf) => {
  const msg = { type: 'post', text: 'Testing!' }
  let state = validate.initial()
  state = validate.appendNew(state, null, alice, msg, Date.now())
  state = validate.appendNew(state, null, bob, msg, Date.now() + 1)

  addMsg(state.queue[0].value, raf, (e1, msg1) => {
    addMsg(state.queue[1].value, raf, (e2, msg2) => {
      query(
        fromDB(db),
        or(
          slowEqual('value.content.type', 'post'),
          deferred((meta, cb) => {
            setTimeout(() => {
              cb(null, slowEqual('value.author', alice.id))
            }, 100)
          })
        ),
        toCallback((err, msgs) => {
          t.error(err, 'toCallback got no error')
          t.equal(msgs.length, 2, 'toCallback got two messages')
          t.equal(msgs[0].value.author, alice.id)
          t.equal(msgs[0].value.content.type, 'post')
          t.equal(msgs[1].value.author, bob.id)
          t.equal(msgs[1].value.content.type, 'post')
          t.end()
        })
      )
    })
  })
})

prepareAndRunTest('support empty deferred operations', dir, (t, db, raf) => {
  const msg = { type: 'post', text: 'Testing!' }
  let state = validate.initial()
  state = validate.appendNew(state, null, alice, msg, Date.now())
  state = validate.appendNew(state, null, bob, msg, Date.now() + 1)

  addMsg(state.queue[0].value, raf, (e1, msg1) => {
    addMsg(state.queue[1].value, raf, (e2, msg2) => {
      query(
        fromDB(db),
        and(
          deferred((meta, cb) => {
            setTimeout(() => {
              cb(null, null)
            }, 100)
          })
        ),
        toCallback((err, msgs) => {
          t.error(err, 'toCallback got no error')
          t.equal(msgs.length, 2, 'toCallback got two messages')
          t.equal(msgs[0].value.author, alice.id)
          t.equal(msgs[0].value.content.type, 'post')
          t.equal(msgs[1].value.author, bob.id)
          t.equal(msgs[1].value.content.type, 'post')
          t.end()
        })
      )
    })
  })
})

prepareAndRunTest('empty deferred AND equal', dir, (t, db, raf) => {
  const msg = { type: 'post', text: 'Testing!' }
  let state = validate.initial()
  state = validate.appendNew(state, null, alice, msg, Date.now())
  state = validate.appendNew(state, null, bob, msg, Date.now() + 1)

  addMsg(state.queue[0].value, raf, (e1, msg1) => {
    addMsg(state.queue[1].value, raf, (e2, msg2) => {
      query(
        fromDB(db),
        and(
          deferred((meta, cb) => {
            setTimeout(cb, 100)
          }),
          slowEqual('value.author', alice.id)
        ),
        toCallback((err, msgs) => {
          t.error(err, 'toCallback got no error')
          t.equal(msgs.length, 1, 'toCallback got one message')
          t.equal(msgs[0].value.author, alice.id)
          t.equal(msgs[0].value.content.type, 'post')
          t.end()
        })
      )
    })
  })
})

prepareAndRunTest('support live offset operations', dir, (t, db, raf) => {
  const msg = { type: 'post', text: 'Testing!' }
  let state = validate.initial()
  state = validate.appendNew(state, null, alice, msg, Date.now())
  state = validate.appendNew(state, null, bob, msg, Date.now())

  var ps = Pushable()

  query(
    fromDB(db),
    live(),
    and(
      deferred((meta, cb) => {
        setTimeout(() => {
          cb(null, liveOffsets(ps))
        }, 100)
      })
    ),
    toPullStream(),
    pull.drain((msg) => {
      t.equal(msg.value.author, alice.id)

      // test we don't get live messages after aborting stream
      addMsg(state.queue[1].value, raf, (e2, msg2) => {
        ps.push(2)
        t.end()
      })

      return false // abort
    })
  )

  addMsg(state.queue[0].value, raf, (e1, msg1) => {
    ps.push(1)
  })
})

prepareAndRunTest(
  'support live only operations pull stream',
  dir,
  (t, db, raf) => {
    const msg = { type: 'post', text: 'Testing!' }
    let state = validate.initial()
    state = validate.appendNew(state, null, alice, msg, Date.now())
    state = validate.appendNew(state, null, bob, msg, Date.now() + 1)

    addMsg(state.queue[0].value, raf, (e1, msg1) => {
      let i = 0
      query(
        fromDB(db),
        and(slowEqual('value.content.type', 'post')),
        live({ old: true }), // to make sure the next one overrides this one
        live(),
        toPullStream(),
        pull.drain((msg) => {
          t.equal(msg.value.author, bob.id)
          t.end()
        })
      )

      // when setting up a query, executeDeferredOps needs to run
      // so we wait 1 tick
      setTimeout(() => {
        addMsg(state.queue[1].value, raf, (e2, msg2) => {})
      })
    })
  }
)

prepareAndRunTest('support live operations async iter', dir, (t, db, raf) => {
  const msg = { type: 'post', text: 'Testing!' }
  let state = validate.initial()
  state = validate.appendNew(state, null, alice, msg, Date.now())
  state = validate.appendNew(state, null, bob, msg, Date.now() + 1)

  addMsg(state.queue[0].value, raf, async (e1, msg1) => {
    let i = 0
    const results = query(
      fromDB(db),
      and(slowEqual('value.content.type', 'post')),
      live(), // to make sure the next one overrides this one
      live({ old: true }),
      toAsyncIter()
    )
    for await (let msg of results) {
      if (i++ == 0) {
        t.equal(msg.value.author, alice.id)
        addMsg(state.queue[1].value, raf, (e2, msg2) => {})
      } else {
        t.equal(msg.value.author, bob.id)
        t.end()
      }
    }
  })
})

prepareAndRunTest('support live operations', dir, (t, db, raf) => {
  const msg = { type: 'post', text: 'Testing!' }
  let state = validate.initial()
  state = validate.appendNew(state, null, alice, msg, Date.now())
  state = validate.appendNew(state, null, bob, msg, Date.now() + 1)

  addMsg(state.queue[0].value, raf, (e1, msg1) => {
    let i = 0
    query(
      fromDB(db),
      and(slowEqual('value.content.type', 'post')),
      live({ old: true }),
      toPullStream(),
      pull.drain((msg) => {
        if (i++ == 0) {
          t.equal(msg.value.author, alice.id)
          addMsg(state.queue[1].value, raf, (e2, msg2) => {})
        } else {
          t.equal(msg.value.author, bob.id)
          t.end()
        }
      })
    )
  })
})

prepareAndRunTest('live empty', dir, (t, db, raf) => {
  const msg = { type: 'post', text: 'Testing!' }
  let state = validate.initial()
  state = validate.appendNew(state, null, alice, msg, Date.now())
  state = validate.appendNew(state, null, bob, msg, Date.now() + 1)

  addMsg(state.queue[0].value, raf, (e1, msg1) => {
    let i = 0
    query(
      fromDB(db),
      live({ old: true }),
      toPullStream(),
      pull.drain((msg) => {
        if (i++ == 0) {
          t.equal(msg.value.author, alice.id)
          addMsg(state.queue[1].value, raf, (e2, msg2) => {})
        } else {
          t.equal(msg.value.author, bob.id)
          t.end()
        }
      })
    )
  })
})

prepareAndRunTest('live AND empty deferred', dir, (t, db, raf) => {
  const msg = { type: 'post', text: 'Testing!' }
  let state = validate.initial()
  state = validate.appendNew(state, null, alice, msg, Date.now())
  state = validate.appendNew(state, null, bob, msg, Date.now() + 1)

  addMsg(state.queue[0].value, raf, (e1, msg1) => {
    let i = 0
    query(
      fromDB(db),
      and(
        deferred((meta, cb) => {
          setTimeout(cb, 100)
        })
      ),
      live({ old: true }),
      toPullStream(),
      pull.drain((msg) => {
        if (i++ == 0) {
          t.equal(msg.value.author, alice.id)
          addMsg(state.queue[1].value, raf, (e2, msg2) => {})
        } else {
          t.equal(msg.value.author, bob.id)
          t.end()
        }
      })
    )
  })
})

prepareAndRunTest('support slowIncludes', dir, (t, db, raf) => {
  const msg1 = { type: 'post', text: '1st', animals: ['cat', 'dog', 'bird'] }
  const msg2 = { type: 'contact', text: '2nd', animals: ['bird'] }
  const msg3 = { type: 'post', text: '3rd', animals: ['cat'] }

  let state = validate.initial()
  state = validate.appendNew(state, null, alice, msg1, Date.now())
  state = validate.appendNew(state, null, alice, msg2, Date.now() + 1)
  state = validate.appendNew(state, null, alice, msg3, Date.now() + 2)

  addMsg(state.queue[0].value, raf, (e1, m1) => {
    addMsg(state.queue[1].value, raf, (e2, m2) => {
      addMsg(state.queue[2].value, raf, (e3, m3) => {
        query(
          fromDB(db),
          and(slowIncludes('value.content.animals', 'bird')),
          toCallback((err, msgs) => {
            t.error(err, 'got no error')
            t.equal(msgs.length, 2, 'got two messages')
            t.equal(msgs[0].value.content.text, '1st')
            t.equal(msgs[1].value.content.text, '2nd')
            t.end()
          })
        )
      })
    })
  })
})

prepareAndRunTest('support slowIncludes and pluck', dir, (t, db, raf) => {
  const msg1 = {
    type: 'post',
    text: '1st',
    animals: [{ word: 'cat' }, { word: 'dog' }, { word: 'bird' }],
  }
  const msg2 = {
    type: 'contact',
    text: '2nd',
    animals: [{ word: 'bird' }],
  }
  const msg3 = {
    type: 'post',
    text: '3rd',
    animals: [{ word: 'cat' }],
  }

  let state = validate.initial()
  state = validate.appendNew(state, null, alice, msg1, Date.now())
  state = validate.appendNew(state, null, alice, msg2, Date.now() + 1)
  state = validate.appendNew(state, null, alice, msg3, Date.now() + 2)

  addMsg(state.queue[0].value, raf, (e1, m1) => {
    addMsg(state.queue[1].value, raf, (e2, m2) => {
      addMsg(state.queue[2].value, raf, (e3, m3) => {
        query(
          fromDB(db),
          and(slowIncludes('value.content.animals', 'cat', { pluck: 'word' })),
          toCallback((err, msgs) => {
            t.error(err, 'got no error')
            t.equal(msgs.length, 2, 'got two messages')
            t.equal(msgs[0].value.content.text, '1st')
            t.equal(msgs[1].value.content.text, '3rd')
            t.end()
          })
        )
      })
    })
  })
})
