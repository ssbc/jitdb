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
      fs.appendFileSync(reportPath, '| Part | Duration |\n|---|---|\n')
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

test('core indexes', (t) => {
  const start = Date.now()
  raf = Log(newLogPath, { blockSize: 64 * 1024 })
  rimraf.sync(indexesDir)
  db = JITDB(raf, indexesDir)
  db.onReady(() => {
    const duration = Date.now() - start
    t.pass(`duration: ${duration}ms`)
    fs.appendFileSync(reportPath, `| Load core indexes | ${duration}ms |\n`)
    t.end()
  })
})

test('query one huge index (first run)', (t) => {
  db.onReady(() => {
    const start = Date.now()
    query(
      fromDB(db),
      where(equal(seekType, 'post', { indexType: 'type' })),
      toCallback((err, msgs) => {
        if (err) t.fail(err)
        const duration = Date.now() - start
        if (msgs.length !== 23310)
          t.fail('msgs.length is wrong: ' + msgs.length)
        t.pass(`duration: ${duration}ms`)
        fs.appendFileSync(
          reportPath,
          `| Query 1 big index (1st run) | ${duration}ms |\n`
        )
        t.end()
      })
    )
  })
})

test('query one huge index (second run)', (t) => {
  db.onReady(() => {
    const start = Date.now()
    query(
      fromDB(db),
      where(equal(seekType, 'post', { indexType: 'type' })),
      toCallback((err, msgs) => {
        if (err) t.fail(err)
        const duration = Date.now() - start
        if (msgs.length !== 23310)
          t.fail('msgs.length is wrong: ' + msgs.length)
        t.pass(`duration: ${duration}ms`)
        fs.appendFileSync(
          reportPath,
          `| Query 1 big index (2nd run) | ${duration}ms |\n`
        )
        t.end()
      })
    )
  })
})

test('count one huge index (third run)', (t) => {
  db.onReady(() => {
    const start = Date.now()
    query(
      fromDB(db),
      where(equal(seekType, 'post', { indexType: 'type' })),
      count(),
      toCallback((err, total) => {
        if (err) t.fail(err)
        const duration = Date.now() - start
        if (total !== 23310) t.fail('total is wrong: ' + total)
        t.pass(`duration: ${duration}ms`)
        fs.appendFileSync(
          reportPath,
          `| Count 1 big index (3rd run) | ${duration}ms |\n`
        )
        t.end()
      })
    )
  })
})

test('create an index twice concurrently', (t) => {
  db.onReady(() => {
    const done = multicb({ pluck: 1 })
    const start = Date.now()

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
      if (err) t.fail(err)
      const duration = Date.now() - start
      t.pass(`duration: ${duration}ms`)
      fs.appendFileSync(
        reportPath,
        `| Create an index twice concurrently | ${duration}ms |\n`
      )
      t.end()
    })
  })
})

test('query three indexes (first run)', (t) => {
  const alice = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))
  const bob = ssbKeys.loadOrCreateSync(path.join(dir, 'secret-b'))
  db.onReady(() => {
    const start = Date.now()
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
        if (err) t.fail(err)
        const duration = Date.now() - start
        if (msgs.length !== 24606)
          t.fail('msgs.length is wrong: ' + msgs.length)
        t.pass(`duration: ${duration}ms`)
        fs.appendFileSync(
          reportPath,
          `| Query 3 indexes (1st run) | ${duration}ms |\n`
        )
        t.end()
      })
    )
  })
})

test('query three indexes (second run)', (t) => {
  const alice = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))
  const bob = ssbKeys.loadOrCreateSync(path.join(dir, 'secret-b'))

  db.onReady(() => {
    const start = Date.now()
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
        if (err) t.fail(err)
        const duration = Date.now() - start
        if (msgs.length !== 24606)
          t.fail('msgs.length is wrong: ' + msgs.length)
        t.pass(`duration: ${duration}ms`)
        fs.appendFileSync(
          reportPath,
          `| Query 3 indexes (2nd run) | ${duration}ms |\n`
        )
        t.end()
      })
    )
  })
})

test('load two indexes concurrently', (t) => {
  const alice = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))
  const bob = ssbKeys.loadOrCreateSync(path.join(dir, 'secret-b'))

  function waitForFile(filename, cb) {
    fs.exists(filename, function(exists) {
      if (exists) cb()
      else setTimeout(() => waitForFile(filename, cb), 250)
    })
  }

  db.onReady(() => {
    const done = multicb({ pluck: 1 })
    const start = Date.now()
    const contact_index_filename = path.join(indexesDir, 'type_contact.index')

    // we need to wait for the other tests to write the index file
    waitForFile(contact_index_filename, () => {
      db.indexes['type_contact'] = {
        offset: 0,
        bitset: new TypedFastBitSet(),
        lazy: true,
        filepath: contact_index_filename,
      }

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
        if (err) t.fail(err)
        const duration = Date.now() - start
        t.pass(`duration: ${duration}ms`)
        fs.appendFileSync(
          reportPath,
          `| Load two indexes concurrently | ${duration}ms |\n`
        )
        t.end()
      })
    })
  })
})

test('paginate big index with small pageSize', (t) => {
  const TOTAL = 20000
  const PAGESIZE = 5
  const NUMPAGES = TOTAL / PAGESIZE
  db.onReady(() => {
    const start = Date.now()
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
          if (err) t.fail(err)
          const duration = Date.now() - start
          if (i !== NUMPAGES) t.fail('wrong number of pages read: ' + i)
          t.pass(`duration: ${duration}ms`)
          fs.appendFileSync(
            reportPath,
            `| Paginate ${TOTAL} msgs with pageSize=${PAGESIZE} | ${duration}ms |\n`
          )
          t.end()
        }
      )
    )
  })
})

test('paginate big index with big pageSize', (t) => {
  const TOTAL = 20000
  const PAGESIZE = 500
  const NUMPAGES = TOTAL / PAGESIZE
  db.onReady(() => {
    const start = Date.now()
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
          if (err) t.fail(err)
          const duration = Date.now() - start
          if (i !== NUMPAGES) t.fail('wrong number of pages read: ' + i)
          t.pass(`duration: ${duration}ms`)
          fs.appendFileSync(
            reportPath,
            `| Paginate ${TOTAL} msgs with pageSize=${PAGESIZE} | ${duration}ms |\n`
          )
          t.end()
        }
      )
    )
  })
})

test('query a prefix map (first run)', (t) => {
  db.onReady(() => {
    query(
      fromDB(db),
      paginate(1),
      toCallback((err, { results }) => {
        if (err) t.fail(err)
        const rootKey = results[0].key

        db.onReady(() => {
          const start = Date.now()
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
                if (err) t.fail(err)
                const duration = Date.now() - start
                if (i !== 92) t.fail('wrong number of pages read: ' + i)
                t.pass(`duration: ${duration}ms`)
                fs.appendFileSync(
                  reportPath,
                  `| Query a prefix map (1st run) | ${duration}ms |\n`
                )
                t.end()
              }
            )
          )
        })
      })
    )
  })
})

test('query a prefix map (second run)', (t) => {
  db.onReady(() => {
    query(
      fromDB(db),
      paginate(1),
      toCallback((err, { results }) => {
        if (err) t.fail(err)
        const rootKey = results[0].key

        db.onReady(() => {
          const start = Date.now()
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
                if (err) t.fail(err)
                const duration = Date.now() - start
                if (i !== 92) t.fail('wrong number of pages read: ' + i)
                t.pass(`duration: ${duration}ms`)
                fs.appendFileSync(
                  reportPath,
                  `| Query a prefix map (2nd run) | ${duration}ms |\n`
                )
                t.end()
              }
            )
          )
        })
      })
    )
  })
})
