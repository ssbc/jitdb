const test = require('tape')
const validate = require('ssb-validate')
const ssbKeys = require('ssb-keys')
const path = require('path')
const { prepareAndRunTest, addMsg, helpers } = require('./common')()
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')

const dir = '/tmp/jitdb-query'
rimraf.sync(dir)
mkdirp.sync(dir)

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
      seek: helpers.seekType,
      value: 'post',
      indexType: 'type',
    },
  }

  const contactQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekType,
      value: 'contact',
      indexType: 'type',
    },
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    addMsg(state.queue[1].value, raf, (err, msg) => {
      addMsg(state.queue[2].value, raf, (err, msg) => {
        db.all(typeQuery, 0, false, (err, results) => {
          t.equal(results.length, 2)
          t.equal(results[0].value.content.type, 'post')
          t.equal(results[1].value.content.type, 'post')

          db.all(contactQuery, 0, false, (err, results) => {
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
  state = validate.appendNew(state, null, keys, msg2, Date.now() + 1)
  state = validate.appendNew(state, null, keys, msg3, Date.now() + 2)

  const typeQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekType,
      value: 'post',
      indexType: 'type',
    },
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    addMsg(state.queue[1].value, raf, (err, msg) => {
      addMsg(state.queue[2].value, raf, (err, msg) => {
        db.paginate(typeQuery, 0, 1, true, (err, results) => {
          t.equal(results.data.length, 1)
          t.equal(results.data[0].value.content.text, 'Testing 2!')
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
  state = validate.appendNew(state, null, keys, msg2, Date.now() + 1)
  state = validate.appendNew(state, null, keys, msg3, Date.now() + 2)

  const typeQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekType,
      value: 'post',
      indexType: 'type',
    },
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    addMsg(state.queue[1].value, raf, (err, msg) => {
      addMsg(state.queue[2].value, raf, (err, msg) => {
        db.paginate(typeQuery, 1, 1, true, (err, results) => {
          t.equal(results.data.length, 1)
          t.equal(results.data[0].value.content.text, 'Testing!')
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
      seek: helpers.seekType,
      value: Buffer.from('post'),
      indexType: 'type',
    },
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    db.paginate(typeQuery, 0, 1, true, (err, results) => {
      t.equal(results.data.length, 1)
      t.equal(results.data[0].value.content.text, 'Testing!')
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
      seek: helpers.seekRoot,
      value: undefined,
      indexType: 'root',
    },
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    addMsg(state.queue[1].value, raf, (err, msg) => {
      db.paginate(typeQuery, 0, 1, true, (err, results) => {
        t.equal(results.data.length, 1)
        t.equal(results.data[0].value.content.text, 'Testing no root')
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
  state = validate.appendNew(state, null, keys, msg2, Date.now() + 1)
  state = validate.appendNew(state, null, keys, msg3, Date.now() + 2)
  state = validate.appendNew(state, null, keys, msg4, Date.now() + 3)

  const filterQuery = {
    type: 'AND',
    data: [
      {
        type: 'GT',
        data: {
          indexName: 'sequence',
          value: 1,
        },
      },
      {
        type: 'EQUAL',
        data: {
          seek: helpers.seekAuthor,
          value: keys.id,
          indexType: 'author',
          indexAll: true,
        },
      },
    ],
  }

  addMsg(state.queue[0].value, raf, (err, dbMsg1) => {
    addMsg(state.queue[1].value, raf, (err, dbMsg2) => {
      addMsg(state.queue[2].value, raf, (err, dbMsg3) => {
        addMsg(state.queue[3].value, raf, (err, dbMsg4) => {
          db.all(filterQuery, 0, false, (err, results) => {
            t.equal(results.length, 3)
            t.equal(results[0].value.content.text, '2')

            filterQuery.data[0].type = 'GTE'
            db.all(filterQuery, 0, false, (err, results) => {
              t.equal(results.length, 4)
              t.equal(results[0].value.content.text, '1')

              filterQuery.data[0].type = 'LT'
              filterQuery.data[0].data.value = 3
              db.all(filterQuery, 0, false, (err, results) => {
                t.equal(results.length, 2)
                t.equal(results[0].value.content.text, '1')

                filterQuery.data[0].type = 'LTE'
                db.all(filterQuery, 0, false, (err, results) => {
                  t.equal(results.length, 3)
                  t.equal(results[0].value.content.text, '1')

                  filterQuery.data[0].type = 'GT'
                  filterQuery.data[0].data.indexName = 'timestamp'
                  filterQuery.data[0].data.value = dbMsg1.value.timestamp
                  db.all(filterQuery, 0, false, (err, results) => {
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

prepareAndRunTest('GTE Zero', dir, (t, db, raf) => {
  const msg1 = { type: 'post', text: '1' }
  const msg2 = { type: 'post', text: '2' }
  const msg3 = { type: 'post', text: '3' }
  const msg4 = { type: 'post', text: '4' }

  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg1, Date.now())
  state = validate.appendNew(state, null, keys, msg2, Date.now() + 1)
  state = validate.appendNew(state, null, keys, msg3, Date.now() + 2)
  state = validate.appendNew(state, null, keys, msg4, Date.now() + 3)

  const filterQuery = {
    type: 'GTE',
    data: {
      indexName: 'sequence',
      value: 0,
    },
  }

  addMsg(state.queue[0].value, raf, (err, dbMsg1) => {
    addMsg(state.queue[1].value, raf, (err, dbMsg2) => {
      addMsg(state.queue[2].value, raf, (err, dbMsg3) => {
        addMsg(state.queue[3].value, raf, (err, dbMsg4) => {
          db.all(filterQuery, 0, false, (err, results) => {
            t.equal(results.length, 4)
            t.equal(results[0].value.content.text, '1')
            t.equal(results[1].value.content.text, '2')
            t.equal(results[2].value.content.text, '3')
            t.equal(results[3].value.content.text, '4')
            t.end()
          })
        })
      })
    })
  })
})

prepareAndRunTest('Data seqs', dir, (t, db, raf) => {
  const msg1 = { type: 'post', text: 'Testing root', root: '1' }
  const msg2 = { type: 'about', name: 'Test' }
  const msg3 = { type: 'post', text: 'Testing no root' }

  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg1, Date.now())
  state = validate.appendNew(state, null, keys, msg2, Date.now())
  state = validate.appendNew(state, null, keys, msg3, Date.now())

  const dataQuery = {
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
        type: 'SEQS',
        seqs: [363, 765],
      },
    ],
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    addMsg(state.queue[1].value, raf, (err, msg) => {
      addMsg(state.queue[2].value, raf, (err, msg) => {
        db.paginate(dataQuery, 0, 1, true, (err, results) => {
          t.equal(results.data.length, 1)
          t.equal(results.data[0].value.content.text, 'Testing no root')
          t.end()
        })
      })
    })
  })
})

prepareAndRunTest('Data offsets simple', dir, (t, db, raf) => {
  const msg1 = { type: 'post', text: 'Testing root', root: '1' }
  const msg2 = { type: 'about', name: 'Test' }
  const msg3 = { type: 'post', text: 'Testing no root' }

  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg1, Date.now())
  state = validate.appendNew(state, null, keys, msg2, Date.now())
  state = validate.appendNew(state, null, keys, msg3, Date.now())

  const dataQuery = {
    type: 'OFFSETS',
    offsets: [1, 2],
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    addMsg(state.queue[1].value, raf, (err, msg) => {
      addMsg(state.queue[2].value, raf, (err, msg) => {
        db.all(dataQuery, 0, false, (err, results) => {
          t.equal(results.length, 2)
          t.equal(results[0].value.content.name, 'Test')
          t.equal(results[1].value.content.text, 'Testing no root')
          t.end()
        })
      })
    })
  })
})

prepareAndRunTest('Data offsets', dir, (t, db, raf) => {
  const msg1 = { type: 'post', text: 'Testing root', root: '1' }
  const msg2 = { type: 'about', name: 'Test' }
  const msg3 = { type: 'post', text: 'Testing no root' }

  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg1, Date.now())
  state = validate.appendNew(state, null, keys, msg2, Date.now())
  state = validate.appendNew(state, null, keys, msg3, Date.now())

  const dataQuery = {
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
        type: 'OFFSETS',
        offsets: [1, 2],
      },
    ],
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    addMsg(state.queue[1].value, raf, (err, msg) => {
      addMsg(state.queue[2].value, raf, (err, msg) => {
        db.paginate(dataQuery, 0, 1, true, (err, results) => {
          t.equal(results.data.length, 1)
          t.equal(results.data[0].value.content.text, 'Testing no root')
          t.end()
        })
      })
    })
  })
})

prepareAndRunTest('Multiple ands', dir, (t, db, raf) => {
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
      seek: helpers.seekType,
      value: 'post',
      indexType: 'type',
    },
  }

  const authorQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekAuthor,
      value: keys.id,
      indexType: 'author',
    },
  }

  const seqQuery = {
    type: 'GT',
    data: {
      indexName: 'sequence',
      value: 1,
    },
  }

  const allQuery = {
    type: 'AND',
    data: [typeQuery, authorQuery, seqQuery],
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    addMsg(state.queue[1].value, raf, (err, msg) => {
      addMsg(state.queue[2].value, raf, (err, msg) => {
        db.all(allQuery, 0, false, (err, results) => {
          t.equal(results.length, 1)
          t.equal(results[0].value.content.text, 'Testing 2!')
          t.end()
        })
      })
    })
  })
})

prepareAndRunTest('Multiple ors', dir, (t, db, raf) => {
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
      seek: helpers.seekType,
      value: 'post',
      indexType: 'type',
    },
  }

  const authorRandomQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekAuthor,
      value: 'random',
      indexType: 'author',
    },
  }

  const authorRandom2Query = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekAuthor,
      value: 'random2',
      indexType: 'author',
    },
  }

  const authorQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekAuthor,
      value: keys.id,
      indexType: 'author',
    },
  }

  const authorsQuery = {
    type: 'OR',
    data: [authorRandomQuery, authorRandom2Query, authorQuery],
  }

  const allQuery = {
    type: 'AND',
    data: [typeQuery, authorsQuery],
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    addMsg(state.queue[1].value, raf, (err, msg) => {
      addMsg(state.queue[2].value, raf, (err, msg) => {
        db.all(allQuery, 0, false, (err, results) => {
          t.equal(results.length, 2)
          t.equal(results[0].value.content.text, 'Testing!')
          t.equal(results[1].value.content.text, 'Testing 2!')
          t.end()
        })
      })
    })
  })
})
