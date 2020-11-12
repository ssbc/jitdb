const test = require('tape')
const path = require('path')
const rimraf = require('rimraf')
const FlumeLog = require('async-flumelog')
const JITDB = require('../index')
const {query, fromDB, and, or, slowEqual, toCallback } = require('../operators')

const fixture = path.join(__dirname, '..', 'fixture')
const logPath = path.join(fixture, 'flume', 'log.bipf')
const indexesDir = path.join(fixture, 'indexes')

const alice = '@Sz83Igu5dM9u2b49SGXmaLq5mRudVIFFawReZYSnQuQ=.ed25519'
const bob = '@WZdJI0tM9IxsIcqDRMnfhEUQtg0uiWb6ojZh1fjFU1Q=.ed25519'

function compare(duration, max) {
  return '' + duration + 'ms is less than ' + max + 'ms'
}

test('install and onReady', (t) => {
  const start = Date.now()
  const raf = FlumeLog(logPath, { blockSize: 64*1024 })
  rimraf.sync(indexesDir)
  const db = JITDB(raf, indexesDir)
  db.onReady(() => {
    const duration = Date.now() - start
    const max = 15
    t.true(duration < max, compare(duration, max))
    t.end()
  })
})

test('one index (first run)', (t) => {
  const raf = FlumeLog(logPath, { blockSize: 64*1024 })
  rimraf.sync(indexesDir)
  const db = JITDB(raf, indexesDir)
  db.onReady(() => {
    const start = Date.now()
    query(
      fromDB(db),
      and(slowEqual('value.content.type', 'post')),
      toCallback((err, msgs) => {
        t.error(err)
        const duration = Date.now() - start
        const max = 11000
        t.true(duration < max, compare(duration, max))
        t.equals(msgs.length, 235587)
        t.end()
      })
    )
  })
})

test('one index (second run)', (t) => {
  const raf = FlumeLog(logPath, { blockSize: 64*1024 })
  const db = JITDB(raf, indexesDir)
  db.onReady(() => {
    const start = Date.now()
    query(
      fromDB(db),
      and(slowEqual('value.content.type', 'post')),
      toCallback((err, msgs) => {
        t.error(err)
        const duration = Date.now() - start
        const max = 4400
        t.true(duration < max, compare(duration, max))
        t.equals(msgs.length, 235587)
        t.end()
      })
    )
  })
})

test('many indexes (first run)', (t) => {
  const raf = FlumeLog(logPath, { blockSize: 64*1024 })
  rimraf.sync(indexesDir)
  const db = JITDB(raf, indexesDir)
  db.onReady(() => {
    const start = Date.now()
    query(
      fromDB(db),
      or(
        slowEqual('value.content.type', 'post'),
        slowEqual('value.content.type', 'contact'),
        slowEqual('value.author', alice),
        slowEqual('value.author', bob)
      ),
      toCallback((err, msgs) => {
        t.error(err)
        const duration = Date.now() - start
        const max = 20000
        t.true(duration < max, compare(duration, max))
        t.equals(msgs.length, 471481)
        t.end()
      })
    )
  })
})

test('many indexes (second run)', (t) => {
  const raf = FlumeLog(logPath, { blockSize: 64*1024 })
  const db = JITDB(raf, indexesDir)
  db.onReady(() => {
    const start = Date.now()
    query(
      fromDB(db),
      or(
        slowEqual('value.content.type', 'post'),
        slowEqual('value.content.type', 'contact'),
        slowEqual('value.author', alice),
        slowEqual('value.author', bob)
      ),
      toCallback((err, msgs) => {
        t.error(err)
        const duration = Date.now() - start
        const max = 7000
        t.true(duration < max, compare(duration, max))
        t.equals(msgs.length, 471481)
        t.end()
      })
    )
  })
})