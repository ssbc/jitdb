const test = require('tape')
const fs = require('fs')
const path = require('path')
const FlumeLog = require('async-flumelog')
const generateFixture = require('ssb-fixtures')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const ssbKeys = require('ssb-keys')
const JITDB = require('../index')
const { query, fromDB, and, or, equal, toCallback } = require('../operators')
const { seekType, seekAuthor } = require('../test/helpers')
const copy = require('../copy-json-to-bipf-async')

const dir = '/tmp/jitdb-benchmark'
rimraf.sync(dir)
mkdirp.sync(dir)

const oldLogPath = path.join(dir, 'flume', 'log.offset')
const newLogPath = path.join(dir, 'flume', 'log.bipf')
const indexesDir = path.join(dir, 'indexes')

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
    t.end()
  })
})

test('move flumelog-offset to async-flumelog', (t) => {
  copy(oldLogPath, newLogPath, (err) => {
    if (err) t.fail(err)
    setTimeout(() => {
      t.true(fs.existsSync(newLogPath), 'log.bipf was created')
      t.end()
    }, 4000)
  })
})

let raf
let db

test('core indexes', (t) => {
  const start = Date.now()
  raf = FlumeLog(newLogPath, { blockSize: 64 * 1024 })
  rimraf.sync(indexesDir)
  db = JITDB(raf, indexesDir)
  db.onReady(() => {
    const duration = Date.now() - start
    t.pass(`duration: ${duration}ms`)
    t.end()
  })
})

test('query one huge index (first run)', (t) => {
  db.onReady(() => {
    const start = Date.now()
    query(
      fromDB(db),
      and(equal(seekType, 'post', { indexType: 'type' })),
      toCallback((err, msgs) => {
        if (err) t.fail(err)
        const duration = Date.now() - start
        if (msgs.length !== 23310) t.fail('msgs.length is wrong')
        t.pass(`duration: ${duration}ms`)
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
      and(equal(seekType, 'post', { indexType: 'type' })),
      toCallback((err, msgs) => {
        if (err) t.fail(err)
        const duration = Date.now() - start
        if (msgs.length !== 23310) t.fail('msgs.length is wrong')
        t.pass(`duration: ${duration}ms`)
        t.end()
      })
    )
  })
})

test('query three indexes (first run)', (t) => {
  const alice = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))
  const bob = ssbKeys.loadOrCreateSync(path.join(dir, 'secret-b'))
  db.onReady(() => {
    const start = Date.now()
    query(
      fromDB(db),
      or(
        and(equal(seekType, 'contact', { indexType: 'type' })),
        and(equal(seekAuthor, alice.id, { indexType: 'author', prefix: 32 })),
        and(equal(seekAuthor, bob.id, { indexType: 'author', prefix: 32 }))
      ),
      toCallback((err, msgs) => {
        if (err) t.fail(err)
        const duration = Date.now() - start
        if (msgs.length !== 24606) t.fail('msgs.length is wrong')
        t.pass(`duration: ${duration}ms`)
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
      or(
        and(equal(seekType, 'contact', { indexType: 'type' })),
        and(equal(seekAuthor, alice.id, { indexType: 'author', prefix: 32 })),
        and(equal(seekAuthor, bob.id, { indexType: 'author', prefix: 32 }))
      ),
      toCallback((err, msgs) => {
        if (err) t.fail(err)
        const duration = Date.now() - start
        if (msgs.length !== 24606) t.fail('msgs.length is wrong')
        t.pass(`duration: ${duration}ms`)
        t.end()
      })
    )
  })
})
