const test = require('tape')
const path = require('path')
const rimraf = require('rimraf')
const FlumeLog = require('async-flumelog')
const JITDB = require('../index')
const cpuStat = require('cpu-stat')
const {query, fromDB, and, or, slowEqual, toCallback } = require('../operators')

const fixture = path.join(__dirname, '..', 'fixture')
const logPath = path.join(fixture, 'flume', 'log.bipf')
const indexesDir = path.join(fixture, 'indexes')

const alice = "@58u/J9+5bOXeYRDCYQ9cJ7kklghIpQFPBYxlhKq1/qs=.ed25519"
const bob = "@0/c5XBu91ciAa+PjmXI5QXkOGPx+iEXO2Q2OKmdne6E=.ed25519"

const avgClockMHz = cpuStat.avgClockMHz()

function compare(val, max) {
  return '' + val + ' is less than ' + max
}

test('install and onReady', (t) => {
  t.pass('CPU clock is on average ' + avgClockMHz + ' MHZ')
  const start = Date.now()
  const raf = FlumeLog(logPath, { blockSize: 64*1024 })
  rimraf.sync(indexesDir)
  const db = JITDB(raf, indexesDir)
  db.onReady(() => {
    const duration = Date.now() - start
    const durationPerMHz = duration/avgClockMHz
    console.log(duration, durationPerMHz)
    const max = 0.004
    t.true(durationPerMHz < max, compare(durationPerMHz, max))
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
        const durationPerMHz = duration/avgClockMHz
        console.log(duration, durationPerMHz)
        const max = 0.25
        t.true(durationPerMHz < max, compare(durationPerMHz, max))
        t.equals(msgs.length, 23310)
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
        const durationPerMHz = duration/avgClockMHz
        console.log(duration, durationPerMHz)
        const max = 0.11
        t.true(durationPerMHz < max, compare(durationPerMHz, max))
        t.equals(msgs.length, 23310)
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
        const durationPerMHz = duration/avgClockMHz
        console.log(duration, durationPerMHz)
        const max = 0.4
        t.true(durationPerMHz < max, compare(durationPerMHz, max))
        t.equals(msgs.length, 47889)
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
        const durationPerMHz = duration/avgClockMHz
        console.log(duration, durationPerMHz)
        const max = 0.17
        t.true(durationPerMHz < max, compare(durationPerMHz, max))
        t.equals(msgs.length, 47889)
        t.end()
      })
    )
  })
})