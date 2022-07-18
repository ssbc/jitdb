// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

const test = require('tape')
const validate = require('ssb-validate')
const ssbKeys = require('ssb-keys')
const path = require('path')
const { prepareAndRunTest, addMsg, helpers } = require('./common')()
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const { safeFilename } = require('../files')
const { readFile, writeFile } = require('atomic-file-rw')

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
  state = validate.appendNew(state, null, keys, msg2, Date.now() + 1)
  state = validate.appendNew(state, null, keys, msg3, Date.now() + 2)

  const typeQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekType,
      value: helpers.toBipf('post'),
      indexType: 'type',
      indexName: 'type_post',
    },
  }

  const contactQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekType,
      value: helpers.toBipf('contact'),
      indexType: 'type',
      indexName: 'type_contact',
    },
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    addMsg(state.queue[1].value, raf, (err, msg) => {
      addMsg(state.queue[2].value, raf, (err, msg) => {
        db.all(typeQuery, 0, false, false, 'declared', (err, results) => {
          t.equal(results.length, 2)
          t.equal(results[0].value.content.type, 'post')
          t.equal(results[1].value.content.type, 'post')

          db.all(contactQuery, 0, false, false, 'declared', (err, results) => {
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
      value: helpers.toBipf('post'),
      indexType: 'type',
      indexName: 'type_post',
    },
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    addMsg(state.queue[1].value, raf, (err, msg) => {
      addMsg(state.queue[2].value, raf, (err, msg) => {
        db.paginate(
          typeQuery,
          0,
          1,
          true,
          false,
          'declared',
          null,
          (err, { results }) => {
            t.equal(results.length, 1)
            t.equal(results[0].value.content.text, 'Testing 2!')
            t.end()
          }
        )
      })
    })
  })
})

prepareAndRunTest('Limit -1', dir, (t, db, raf) => {
  const msg1 = { type: 'post', text: 'Testing limit -1' }

  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg1, Date.now())

  const typeQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekType,
      value: helpers.toBipf('post'),
      indexType: 'type',
      indexName: 'type_post',
    },
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    db.paginate(
      typeQuery,
      0,
      -1,
      true,
      false,
      'declared',
      null,
      (err2, { results }) => {
        t.error(err2)
        t.equal(results.length, 0)
        t.end()
      }
    )
  })
})

prepareAndRunTest('Limit 0', dir, (t, db, raf) => {
  const msg1 = { type: 'post', text: 'Testing limit 0' }

  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg1, Date.now())

  const typeQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekType,
      value: helpers.toBipf('post'),
      indexType: 'type',
      indexName: 'type_post',
    },
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    db.paginate(
      typeQuery,
      0,
      0,
      true,
      false,
      'declared',
      null,
      (err2, { results }) => {
        t.error(err2)
        t.equal(results.length, 0)
        t.end()
      }
    )
  })
})

prepareAndRunTest('Includes', dir, (t, db, raf) => {
  const msg1 = { type: 'post', text: '1st', animals: ['cat', 'dog', 'bird'] }
  const msg2 = { type: 'contact', text: '2nd', animals: ['bird'] }
  const msg3 = { type: 'post', text: '3rd', animals: ['cat'] }

  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg1, Date.now())
  state = validate.appendNew(state, null, keys, msg2, Date.now() + 1)
  state = validate.appendNew(state, null, keys, msg3, Date.now() + 2)

  const typeQuery = {
    type: 'INCLUDES',
    data: {
      seek: helpers.seekAnimals,
      value: helpers.toBipf('bird'),
      indexType: 'animals',
      indexName: 'animals_bird',
    },
  }

  addMsg(state.queue[0].value, raf, (err1, msg) => {
    addMsg(state.queue[1].value, raf, (err2, msg) => {
      addMsg(state.queue[2].value, raf, (err3, msg) => {
        db.all(typeQuery, 0, false, false, 'declared', (err4, results) => {
          t.error(err4)
          t.equal(results.length, 2)
          t.equal(results[0].value.content.text, '1st')
          t.equal(results[1].value.content.text, '2nd')
          t.end()
        })
      })
    })
  })
})

prepareAndRunTest('Includes and pluck', dir, (t, db, raf) => {
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
  state = validate.appendNew(state, null, keys, msg1, Date.now())
  state = validate.appendNew(state, null, keys, msg2, Date.now() + 1)
  state = validate.appendNew(state, null, keys, msg3, Date.now() + 2)

  const typeQuery = {
    type: 'INCLUDES',
    data: {
      seek: helpers.seekAnimals,
      value: helpers.toBipf('bird'),
      indexType: 'animals_word',
      indexName: 'animals_word_bird',
      pluck: helpers.pluckWord,
    },
  }

  addMsg(state.queue[0].value, raf, (err1, msg) => {
    addMsg(state.queue[1].value, raf, (err2, msg) => {
      addMsg(state.queue[2].value, raf, (err3, msg) => {
        db.all(typeQuery, 0, false, false, 'declared', (err4, results) => {
          t.error(err4)
          t.equal(results.length, 2)
          t.equal(results[0].value.content.text, '1st')
          t.equal(results[1].value.content.text, '2nd')
          t.end()
        })
      })
    })
  })
})

prepareAndRunTest('Paginate many pages', dir, (t, db, raf) => {
  const msg1 = { type: 'post', text: '1st' }
  const msg2 = { type: 'post', text: '2nd' }
  const msg3 = { type: 'post', text: '3rd' }

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
      indexName: 'type_post',
    },
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    addMsg(state.queue[1].value, raf, (err, msg) => {
      addMsg(state.queue[2].value, raf, (err, msg) => {
        db.paginate(
          typeQuery,
          0,
          1,
          false,
          false,
          'declared',
          null,
          (err, { results }) => {
            t.equal(results.length, 1)
            t.equal(results[0].value.content.text, '1st')
            db.paginate(
              typeQuery,
              1,
              1,
              false,
              false,
              'declared',
              null,
              (err, { results }) => {
                t.equal(results.length, 1)
                t.equal(results[0].value.content.text, '2nd')
                db.paginate(
                  typeQuery,
                  2,
                  1,
                  false,
                  false,
                  'declared',
                  null,
                  (err, { results }) => {
                    t.equal(results.length, 1)
                    t.equal(results[0].value.content.text, '3rd')
                    t.end()
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

prepareAndRunTest('Paginate empty', dir, (t, db, raf) => {
  const msg1 = { type: 'post', text: '1st' }
  const msg2 = { type: 'post', text: '2nd' }

  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg1, Date.now())
  state = validate.appendNew(state, null, keys, msg2, Date.now() + 1)

  const typeQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekType,
      value: helpers.toBipf('blog'),
      indexType: 'type',
      indexName: 'type_blog',
    },
  }

  addMsg(state.queue[0].value, raf, (err1, msg) => {
    addMsg(state.queue[1].value, raf, (err2, msg) => {
      db.paginate(
        typeQuery,
        0,
        1,
        false,
        false,
        'declared',
        null,
        (err3, { results }) => {
          t.error(err3)
          t.equal(results.length, 0)
          t.end()
        }
      )
    })
  })
})

prepareAndRunTest('Seq', dir, (t, db, raf) => {
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
      indexName: 'type_post',
    },
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    addMsg(state.queue[1].value, raf, (err, msg) => {
      addMsg(state.queue[2].value, raf, (err, msg) => {
        db.paginate(
          typeQuery,
          1,
          1,
          true,
          false,
          'declared',
          null,
          (err, { results }) => {
            t.equal(results.length, 1)
            t.equal(results[0].value.content.text, 'Testing!')
            t.end()
          }
        )
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
      value: helpers.toBipf('post'),
      indexType: 'type',
      indexName: 'type_post',
    },
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    db.paginate(
      typeQuery,
      0,
      1,
      true,
      false,
      'declared',
      null,
      (err, { results }) => {
        t.equal(results.length, 1)
        t.equal(results[0].value.content.text, 'Testing!')
        t.end()
      }
    )
  })
})

prepareAndRunTest('Undefined', dir, (t, db, raf) => {
  const msg1 = { type: 'post', text: 'Testing root', root: '1' }
  const msg2 = { type: 'post', text: 'Testing no root' }
  const msg3 = { type: 'post', text: 'Testing root undefined', root: undefined }

  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg1, Date.now())
  state = validate.appendNew(state, null, keys, msg2, Date.now() + 1)
  state = validate.appendNew(state, null, keys, msg3, Date.now() + 2)

  const typeQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekRoot,
      value: helpers.toBipf(undefined),
      indexType: 'root',
      indexName: 'root_',
    },
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    addMsg(state.queue[1].value, raf, (err, msg) => {
      addMsg(state.queue[2].value, raf, (err, msg) => {
        db.paginate(
          typeQuery,
          0,
          10,
          false,
          false,
          'declared',
          null,
          (err, { results }) => {
            t.equal(results.length, 2)
            t.equal(results[0].value.content.text, 'Testing no root')
            t.equal(results[1].value.content.text, 'Testing root undefined')
            t.end()
          }
        )
      })
    })
  })
})

prepareAndRunTest('Null', dir, (t, db, raf) => {
  const msg1 = { type: 'post', text: 'Testing root', root: '1' }
  const msg2 = { type: 'post', text: 'Testing no root' }
  const msg3 = { type: 'post', text: 'Testing root null', root: null }

  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg1, Date.now())
  state = validate.appendNew(state, null, keys, msg2, Date.now() + 1)
  state = validate.appendNew(state, null, keys, msg3, Date.now() + 1)

  const typeQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekRoot,
      value: helpers.toBipf(null),
      indexType: 'root',
      indexName: 'root_',
    },
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    addMsg(state.queue[1].value, raf, (err, msg) => {
      addMsg(state.queue[2].value, raf, (err, msg) => {
        db.paginate(
          typeQuery,
          0,
          10,
          false,
          false,
          'declared',
          null,
          (err, { results }) => {
            t.equal(results.length, 1)
            t.equal(results[0].value.content.text, 'Testing root null')
            t.end()
          }
        )
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

  let filterQuery = {
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
          value: helpers.toBipf(keys.id),
          indexType: 'author',
          indexAll: true,
          indexName: safeFilename('author_' + keys.id),
        },
      },
    ],
  }

  addMsg(state.queue[0].value, raf, (err, dbMsg1) => {
    addMsg(state.queue[1].value, raf, (err, dbMsg2) => {
      addMsg(state.queue[2].value, raf, (err, dbMsg3) => {
        addMsg(state.queue[3].value, raf, (err, dbMsg4) => {
          db.all(filterQuery, 0, false, false, 'declared', (err, results) => {
            t.error(err, 'no err')
            t.equal(results.length, 3)
            t.equal(results[0].value.content.text, '2')

            filterQuery.data[0].type = 'GTE'
            // clone to force cache invalidation inside db.all:
            filterQuery = Object.assign({}, filterQuery)
            db.all(filterQuery, 0, false, false, 'declared', (err, results) => {
              t.equal(results.length, 4)
              t.equal(results[0].value.content.text, '1')

              filterQuery.data[0].type = 'LT'
              filterQuery.data[0].data.value = 3
              // clone to force cache invalidation inside db.all:
              filterQuery = Object.assign({}, filterQuery)
              db.all(
                filterQuery,
                0,
                false,
                false,
                'declared',
                (err, results) => {
                  t.equal(results.length, 2)
                  t.equal(results[0].value.content.text, '1')

                  filterQuery.data[0].type = 'LTE'
                  // clone to force cache invalidation inside db.all:
                  filterQuery = Object.assign({}, filterQuery)
                  db.all(
                    filterQuery,
                    0,
                    false,
                    false,
                    'declared',
                    (err, results) => {
                      t.equal(results.length, 3)
                      t.equal(results[0].value.content.text, '1')

                      filterQuery.data[0].type = 'GT'
                      filterQuery.data[0].data.indexName = 'timestamp'
                      filterQuery.data[0].data.value = dbMsg1.value.timestamp
                      // clone to force cache invalidation inside db.all:
                      filterQuery = Object.assign({}, filterQuery)
                      db.all(
                        filterQuery,
                        0,
                        false,
                        false,
                        'declared',
                        (err, results) => {
                          t.equal(results.length, 3)
                          t.equal(results[0].value.content.text, '2')

                          t.end()
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
          db.all(filterQuery, 0, false, false, 'declared', (err, results) => {
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

prepareAndRunTest('Data offsets', dir, (t, db, raf) => {
  const msg1 = { type: 'post', text: 'Testing root', root: '1' }
  const msg2 = { type: 'about', name: 'Test' }
  const msg3 = { type: 'post', text: 'Testing no root' }

  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg1, Date.now())
  state = validate.appendNew(state, null, keys, msg2, Date.now() + 1)
  state = validate.appendNew(state, null, keys, msg3, Date.now() + 2)

  const dataQuery = {
    type: 'AND',
    data: [
      {
        type: 'EQUAL',
        data: {
          seek: helpers.seekType,
          value: helpers.toBipf('post'),
          indexType: 'type',
          indexName: 'type_post',
        },
      },
      {
        type: 'OFFSETS',
        offsets: [363, 765],
      },
    ],
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    addMsg(state.queue[1].value, raf, (err, msg) => {
      addMsg(state.queue[2].value, raf, (err, msg) => {
        db.paginate(
          dataQuery,
          0,
          1,
          true,
          false,
          'declared',
          null,
          (err, { results }) => {
            t.equal(results.length, 1)
            t.equal(results[0].value.content.text, 'Testing no root')
            t.end()
          }
        )
      })
    })
  })
})

prepareAndRunTest('Data seqs simple', dir, (t, db, raf) => {
  const msg1 = { type: 'post', text: 'Testing root', root: '1' }
  const msg2 = { type: 'about', name: 'Test' }
  const msg3 = { type: 'post', text: 'Testing no root' }

  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg1, Date.now())
  state = validate.appendNew(state, null, keys, msg2, Date.now() + 1)
  state = validate.appendNew(state, null, keys, msg3, Date.now() + 2)

  const dataQuery = {
    type: 'SEQS',
    seqs: [1, 2],
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    addMsg(state.queue[1].value, raf, (err, msg) => {
      addMsg(state.queue[2].value, raf, (err, msg) => {
        db.all(dataQuery, 0, false, false, 'declared', (err, results) => {
          t.equal(results.length, 2)
          t.equal(results[0].value.content.name, 'Test')
          t.equal(results[1].value.content.text, 'Testing no root')
          t.end()
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
  state = validate.appendNew(state, null, keys, msg2, Date.now() + 1)
  state = validate.appendNew(state, null, keys, msg3, Date.now() + 2)

  const dataQuery = {
    type: 'AND',
    data: [
      {
        type: 'EQUAL',
        data: {
          seek: helpers.seekType,
          value: helpers.toBipf('post'),
          indexType: 'type',
          indexName: 'type_post',
        },
      },
      {
        type: 'SEQS',
        seqs: [1, 2],
      },
    ],
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    addMsg(state.queue[1].value, raf, (err, msg) => {
      addMsg(state.queue[2].value, raf, (err, msg) => {
        db.paginate(
          dataQuery,
          0,
          1,
          true,
          false,
          'declared',
          null,
          (err, { results }) => {
            t.equal(results.length, 1)
            t.equal(results[0].value.content.text, 'Testing no root')
            t.end()
          }
        )
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
  state = validate.appendNew(state, null, keys, msg2, Date.now() + 1)
  state = validate.appendNew(state, null, keys, msg3, Date.now() + 2)

  const typeQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekType,
      value: helpers.toBipf('post'),
      indexType: 'type',
      indexName: 'type_post',
    },
  }

  const authorQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekAuthor,
      value: helpers.toBipf(keys.id),
      indexType: 'author',
      indexName: 'author_' + keys.id,
    },
  }

  const sequenceQuery = {
    type: 'GT',
    data: {
      indexName: 'sequence',
      value: 1,
    },
  }

  const allQuery = {
    type: 'AND',
    data: [typeQuery, authorQuery, sequenceQuery],
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    addMsg(state.queue[1].value, raf, (err, msg) => {
      addMsg(state.queue[2].value, raf, (err, msg) => {
        db.all(allQuery, 0, false, false, 'declared', (err, results) => {
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
  state = validate.appendNew(state, null, keys, msg2, Date.now() + 1)
  state = validate.appendNew(state, null, keys, msg3, Date.now() + 2)

  const typeQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekType,
      value: helpers.toBipf('post'),
      indexType: 'type',
      indexName: 'type_post',
    },
  }

  const authorRandomQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekAuthor,
      value: helpers.toBipf('random'),
      indexType: 'author',
      indexName: 'author_random',
    },
  }

  const authorRandom2Query = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekAuthor,
      value: helpers.toBipf('random2'),
      indexType: 'author',
      indexName: 'author_random2',
    },
  }

  const authorQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekAuthor,
      value: helpers.toBipf(keys.id),
      indexType: 'author',
      indexName: 'author_' + keys.id,
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
        db.all(allQuery, 0, false, false, 'declared', (err, results) => {
          t.equal(results.length, 2)
          t.equal(results[0].value.content.text, 'Testing!')
          t.equal(results[1].value.content.text, 'Testing 2!')
          t.end()
        })
      })
    })
  })
})

prepareAndRunTest('Timestamp discontinuity', dir, (t, db, raf) => {
  const msg1 = { type: 'post', text: '1st' }
  const msg2 = { type: 'post', text: '2nd' }
  const msg3 = { type: 'post', text: '3rd' }

  const start = Date.now()

  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg1, start + 3000)
  state = validate.appendNew(state, null, keys, msg2, start + 2000)
  state = validate.appendNew(state, null, keys, msg3, start + 1000)

  const authorQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekAuthor,
      value: helpers.toBipf(keys.id),
      indexType: 'author',
      indexName: 'author_me',
    },
  }

  // we need to wait for the declared timestamps to win over arrival
  setTimeout(() => {
    addMsg(state.queue[0].value, raf, (err, m1) => {
      addMsg(state.queue[1].value, raf, (err, m2) => {
        addMsg(state.queue[2].value, raf, (err, m3) => {
          db.all(authorQuery, 0, false, false, 'declared', (err, results) => {
            t.equal(results.length, 3)
            t.equal(results[0].value.content.text, '3rd', '3rd ok')
            t.equal(results[1].value.content.text, '2nd', '2nd ok')
            t.equal(results[2].value.content.text, '1st', '1st ok')
            t.end()
          })
        })
      })
    })
  }, 3000)
})

prepareAndRunTest('reindex corrupt indexes', dir, (t, db, raf) => {
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
      indexName: 'type_post',
    },
  }

  function corruptFile(filename, cb) {
    readFile(filename, (err, b) => {
      b.writeUInt32LE(123456, 4 * 4)
      writeFile(filename, b, cb)
    })
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    addMsg(state.queue[1].value, raf, (err, msg) => {
      addMsg(state.queue[2].value, raf, (err, msg) => {
        db.all(typeQuery, 0, false, false, 'declared', (err, results) => {
          t.equal(results.length, 2)

          const dir = '/tmp/jitdb-query/indexesreindex corrupt indexes/'

          setTimeout(() => {
            // wait for save
            corruptFile(dir + 'seq.index', () => {
              corruptFile(dir + 'type_post.index', () => {
                // reload
                db = require('../index')(raf, dir)
                db.onReady(() => {
                  db.all(
                    typeQuery,
                    0,
                    false,
                    false,
                    'declared',
                    (err, results) => {
                      t.equal(results.length, 2)
                      t.end()
                    }
                  )
                })
              })
            })
          }, 2000)
        })
      })
    })
  })
})
