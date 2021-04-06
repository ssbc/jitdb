const test = require('tape')
const fs = require('fs')
const path = require('path')
const pull = require('pull-stream')
const Log = require('async-append-only-log')
const generateFixture = require('ssb-fixtures')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const multicb = require('multicb')
const ssbKeys = require('ssb-keys')
const TypedFastBitSet = require('typedfastbitset')
const runBenchmark = require('./helpers/run_benchmark');
const JITDB = require('../index')
const {
  query,
  fromDB,
  where,
  or,
  equal,
  count,
  toCallback,
  toPullStream,
  paginate,
} = require('../operators')
const { seekType, seekAuthor, seekVoteLink } = require('../test/helpers')
const copy = require('../copy-json-to-bipf-async')

const dir = '/tmp/jitdb-benchmark'
const oldLogPath = path.join(dir, 'flume', 'log.offset')
const newLogPath = path.join(dir, 'flume', 'log.bipf')
const reportPath = path.join(dir, 'benchmark.md')
const indexesDir = path.join(dir, 'indexes')
const alice = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))
const bob = ssbKeys.loadOrCreateSync(path.join(dir, 'secret-b'))

const skipCreate = process.argv[2] === 'noCreate'

if (!skipCreate) {
  rimraf.sync(dir)
  mkdirp.sync(dir)

  const SEED = 'sloop'
  const MESSAGES = 100000
  const AUTHORS = 2000

  test('generate fixture with flumelog-offset', (t) => {
    generateFixture({
      outputDir: dir,
      seed: SEED,
      messages: MESSAGES,
      authors: AUTHORS,
      slim: true,
    }).then(() => {
      t.pass(`seed = ${SEED}`)
      t.pass(`messages = ${MESSAGES}`)
      t.pass(`authors = ${AUTHORS}`)
      t.true(fs.existsSync(oldLogPath), 'log.offset was created')
      fs.appendFileSync(reportPath, '## Benchmark results\n\n')
      fs.appendFileSync(reportPath, '| Part | Speed | Heap Change |\n|---|---|---|\n')
      t.end()
    })
  })

  test('move flumelog-offset to async-log', (t) => {
    copy(oldLogPath, newLogPath, (err) => {
      if (err) t.fail(err)
      setTimeout(() => {
        t.true(fs.existsSync(newLogPath), 'log.bipf was created')
        t.end()
      }, 4000)
    })
  })
}

let raf
let db

const getJitdbReady = (cb) => {
  raf = Log(newLogPath, { blockSize: 64 * 1024 })
  rimraf.sync(indexesDir)
  db = JITDB(raf, indexesDir)
  db.onReady((err) => {
    cb(err)
  })
};

const closeLog = (cb) => {
  if (raf) {
    raf.close(cb)
  } else {
    cb()
  }
}

test('core indexes', (t) => {
  runBenchmark(
    'Load core indexes',
    getJitdbReady,
    closeLog,
    (err, result) => {
      closeLog((err2) => {
        if (err || err2) {
          t.fail(err || err2)
        } else {
          fs.appendFileSync(reportPath, result)
          t.pass(result)
        }
        t.end()
      })
    }
  )
})

const runHugeIndexQuery = (cb) => {
  query(
    fromDB(db),
    where(equal(seekType, 'post', { indexType: 'type' })),
    toCallback((err, msgs) => {
      if (err) {
        cb(err)
      } else if (msgs.length !== 23310) {
        cb(new Error('msgs.length is wrong: ' + msgs.length))
      }
      cb()
    })
  )
}

test('query one huge index (first run)', (t) => {
  runBenchmark(
    'Query 1 big index (1st run)',
    runHugeIndexQuery,
    (cb) => {
      closeLog((err) => {
        if (err) cb(err)
        else getJitdbReady(cb)
      })
    },
    (err, result) => {
      if (err) {
        t.fail(err)
      } else {
        fs.appendFileSync(reportPath, result)
        t.pass(result)
      }
      t.end()
    }
  )
})

test('query one huge index (second run)', (t) => {
  runBenchmark(
    'Query 1 big index (2nd run)',
    runHugeIndexQuery,
    (cb) => {
      closeLog((err) => {
        if (err) cb(err)
        else getJitdbReady((err2) => {
          if (err2) cb(err2)
          else runHugeIndexQuery(cb)
        })
      })
    },
    (err, result) => {
      if (err) {
        t.fail(err)
      } else {
        fs.appendFileSync(reportPath, result)
        t.pass(result)
      }
      t.end()
    }
  )
})

test('count one huge index (third run)', (t) => {
  runBenchmark(
    'Count 1 big index (3rd run)',
    (cb) => {
      query(
        fromDB(db),
        where(equal(seekType, 'post', { indexType: 'type' })),
        count(),
        toCallback((err, total) => {
          if (err) {
            cb(err)
          } else if (total !== 23310) {
            cb(new Error('total is wrong: ' + total))
          }
          cb()
        })
      )
    },
    (cb) => {
      closeLog((err) => {
        if (err) cb(err)
        else getJitdbReady((err2) => {
          if (err2) cb(err2)
          else runHugeIndexQuery((err3) => {
            if (err3) cb(err3)
            else runHugeIndexQuery(cb)
          })
        })
      })
    },
    (err, result) => {
      if (err) {
        t.fail(err)
      } else {
        fs.appendFileSync(reportPath, result)
        t.pass(result)
      }
      t.end()
    }
  )
})

test('create an index twice concurrently', (t) => {
  let done
  runBenchmark(
    'Create an index twice concurrently',
    (cb) => {
      query(
        fromDB(db),
        where(equal(seekType, 'about', { indexType: 'type' })),
        toCallback(done())
      )

      query(
        fromDB(db),
        where(equal(seekType, 'about', { indexType: 'type' })),
        toCallback(done())
      )

      done((err) => {
        if (err) cb(err)
        else cb()
      })
    },
    (cb) => {
      done = multicb({ pluck: 1 })
      closeLog((err) => {
        if (err) cb(err)
        else getJitdbReady(cb)
      })
    },
    (err, result) => {
      if (err) {
        t.fail(err)
      } else {
        fs.appendFileSync(reportPath, result)
        t.pass(result)
      }
      t.end()
    }
  )
})

const runThreeIndexQuery = (cb) => {
  query(
    fromDB(db),
    where(
      or(
        equal(seekType, 'contact', { indexType: 'type' }),
        equal(seekAuthor, alice.id, {
          indexType: 'author',
          prefix: 32,
          prefixOffset: 1,
        }),
        equal(seekAuthor, bob.id, {
          indexType: 'author',
          prefix: 32,
          prefixOffset: 1,
        })
      )
    ),
    toCallback((err, msgs) => {
      if (err) cb(err)
      else if (msgs.length !== 24606)
        cb(new Error('msgs.length is wrong: ' + msgs.length))
      else cb()
    })
  )
}

test('query three indexes (first run)', (t) => {
  runBenchmark(
    'Query 3 indexes (1st run)',
    runThreeIndexQuery,
    (cb) => {
      closeLog((err) => {
        if (err) cb(err)
        else getJitdbReady(cb)
      })
    },
    (err, result) => {
      if (err) {
        t.fail(err)
      } else {
        fs.appendFileSync(reportPath, result)
        t.pass(result)
      }
      t.end()
    }
  )
})

test('query three indexes (second run)', (t) => {
  runBenchmark(
    'Query 3 indexes (2nd run)',
    runThreeIndexQuery,
    (cb) => {
      closeLog((err) => {
        if (err) cb(err)
        else getJitdbReady((err) => {
          if (err) cb(err)
          else runThreeIndexQuery(cb)
        })
      })
    },
    (err, result) => {
      if (err) {
        t.fail(err)
      } else {
        fs.appendFileSync(reportPath, result)
        t.pass(result)
      }
      t.end()
    }
  )
})

test('load two indexes concurrently', (t) => {
  let done
  runBenchmark(
    'Load two indexes concurrently',
    (cb) => {
      query(
        fromDB(db),
        where(
          or(
            equal(seekType, 'contact', { indexType: 'type' }),
            equal(seekAuthor, alice.id, {
              indexType: 'author',
              prefix: 32,
              prefixOffset: 1,
            }),
            equal(seekAuthor, bob.id, {
              indexType: 'author',
              prefix: 32,
              prefixOffset: 1,
            })
          )
        ),
        toCallback(done())
      )

      query(
        fromDB(db),
        where(
          or(
            equal(seekType, 'contact', { indexType: 'type' }),
            equal(seekAuthor, alice.id, {
              indexType: 'author',
              prefix: 32,
              prefixOffset: 1,
            }),
            equal(seekAuthor, bob.id, {
              indexType: 'author',
              prefix: 32,
              prefixOffset: 1,
            })
          )
        ),
        toCallback(done())
      )

      done((err) => {
        if (err) cb(err)
        else cb()
      })
    },
    (cb) => {
      closeLog((err) => {
        if (err) cb(err)
        else getJitdbReady((err) => {
          if (err) cb(err)
          else runThreeIndexQuery((err) => {
            if (err) cb(err)
            else {
              done = multicb({ pluck: 1 })
              db.indexes['type_contact'] = {
                offset: 0,
                bitset: new TypedFastBitSet(),
                lazy: true,
                filepath: path.join(indexesDir, 'type_contact.index'),
              }
              cb()
            }
          })
        })
      })
    },
    (err, result) => {
      if (err) {
        t.fail(err)
      } else {
        fs.appendFileSync(reportPath, result)
        t.pass(result)
      }
      t.end()
    }
  )
})

test('paginate big index with small pageSize', (t) => {
  const TOTAL = 20000
  const PAGESIZE = 5
  const NUMPAGES = TOTAL / PAGESIZE
  runBenchmark(
    `Paginate ${TOTAL} msgs with pageSize=${PAGESIZE}`,
    (cb) => {
      let i = 0
      pull(
        query(
          fromDB(db),
          where(equal(seekType, 'post', { indexType: 'type' })),
          paginate(PAGESIZE),
          toPullStream()
        ),
        pull.take(NUMPAGES),
        pull.drain(
          (msgs) => {
            i++
          },
          (err) => {
            if (err) cb(err)
            else if (i !== NUMPAGES) cb(new Error('wrong number of pages read: ' + i))
            else cb()
          }
        )
      )
    },
    (cb) => {
      closeLog((err) => {
        if (err) cb(err)
        else getJitdbReady((err) => {
          if (err) cb(err)
          else runThreeIndexQuery((err) => {
            if (err) cb(err)
            else {
              done = multicb({ pluck: 1 })
              db.indexes['type_contact'] = {
                offset: 0,
                bitset: new TypedFastBitSet(),
                lazy: true,
                filepath: path.join(indexesDir, 'type_contact.index'),
              }
              cb()
            }
          })
        })
      })
    },
    (err, result) => {
      if (err) {
        t.fail(err)
      } else {
        fs.appendFileSync(reportPath, result)
        t.pass(result)
      }
      t.end()
    }
  )
})

test('paginate big index with big pageSize', (t) => {
  const TOTAL = 20000
  const PAGESIZE = 500
  const NUMPAGES = TOTAL / PAGESIZE
  runBenchmark(
    `Paginate ${TOTAL} msgs with pageSize=${PAGESIZE}`,
    (cb) => {
      let i = 0
      pull(
        query(
          fromDB(db),
          where(equal(seekType, 'post', { indexType: 'type' })),
          paginate(PAGESIZE),
          toPullStream()
        ),
        pull.take(NUMPAGES),
        pull.drain(
          (msgs) => {
            i++
          },
          (err) => {
            if (err) cb(err)
            else if (i !== NUMPAGES) cb(new Error('wrong number of pages read: ' + i))
            else cb()
          }
        )
      )
    },
    (cb) => {
      closeLog((err) => {
        if (err) cb(err)
        else getJitdbReady((err) => {
          if (err) cb(err)
          else runThreeIndexQuery((err) => {
            if (err) cb(err)
            else {
              done = multicb({ pluck: 1 })
              db.indexes['type_contact'] = {
                offset: 0,
                bitset: new TypedFastBitSet(),
                lazy: true,
                filepath: path.join(indexesDir, 'type_contact.index'),
              }
              cb()
            }
          })
        })
      })
    },
    (err, result) => {
      if (err) {
        t.fail(err)
      } else {
        fs.appendFileSync(reportPath, result)
        t.pass(result)
      }
      t.end()
    }
  )
})

const getPrefixMapQueries = () => {
  let rootKey
  return {
    prepareRootKey: (cb) => {
      query(
        fromDB(db),
        paginate(1),
        toCallback((err, { results }) => {
          if (err) cb(err)
          else {
            rootKey = results[0].key
            cb()
          }
        })
      )
    },
    queryMap: (cb) => {
      let i = 0
      pull(
        query(
          fromDB(db),
          where(
            equal(seekVoteLink, rootKey, {
              indexType: 'value_content_vote_link',
              useMap: true,
              prefix: 32,
              prefixOffset: 1,
            })
          ),
          paginate(5),
          toPullStream()
        ),
        pull.drain(
          (msgs) => {
            i++
          },
          (err) => {
            if (err) cb(err)
            else if (i !== 92) cb(new Error('wrong number of pages read: ' + i))
            else cb()
          }
        )
      )
    },
  }
}

test('query a prefix map (first run)', (t) => {
  const { prepareRootKey, queryMap } = getPrefixMapQueries()
  runBenchmark(
    'Query a prefix map (1st run)',
    queryMap,
    (cb) => {
      closeLog((err) => {
        if (err) cb(err)
        else getJitdbReady((err) => {
          if (err) cb(err)
          else runThreeIndexQuery((err) => {
            if (err) cb(err)
            else {
              done = multicb({ pluck: 1 })
              db.indexes['type_contact'] = {
                offset: 0,
                bitset: new TypedFastBitSet(),
                lazy: true,
                filepath: path.join(indexesDir, 'type_contact.index'),
              }
              prepareRootKey(cb)
            }
          })
        })
      })
    },
    (err, result) => {
      if (err) {
        t.fail(err)
      } else {
        fs.appendFileSync(reportPath, result)
        t.pass(result)
      }
      t.end()
    }
  )
})

test('query a prefix map (second run)', (t) => {
  const { prepareRootKey, queryMap } = getPrefixMapQueries()
  runBenchmark(
    'Query a prefix map (2nd run)',
    queryMap,
    (cb) => {
      closeLog((err) => {
        if (err) cb(err)
        else getJitdbReady((err) => {
          if (err) cb(err)
          else runThreeIndexQuery((err) => {
            if (err) cb(err)
            else {
              done = multicb({ pluck: 1 })
              db.indexes['type_contact'] = {
                offset: 0,
                bitset: new TypedFastBitSet(),
                lazy: true,
                filepath: path.join(indexesDir, 'type_contact.index'),
              }
              prepareRootKey((err3) => {
                if (err3) cb(err3)
                else queryMap(cb)
              })
            }
          })
        })
      })
    },
    (err, result) => {
      if (err) {
        t.fail(err)
      } else {
        fs.appendFileSync(reportPath, result)
        t.pass(result)
      }
      t.end()
    }
  )
})
