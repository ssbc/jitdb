const Obv = require('obz')

module.exports = function Status() {
  const indexesStatus = {}
  const indexesLastTime = {}
  const obv = Obv()
  obv.set(indexesStatus)
  const EMIT_INTERVAL = 1000 // ms
  const PRUNE_INTERVAL = 2000 // ms
  let i = 0
  let iTimer = 0
  let timer = null

  function pruneStatus() {
    const now = Date.now()
    for (const indexName in indexesStatus) {
      if (indexesLastTime[indexName] + PRUNE_INTERVAL < now) {
        delete indexesStatus[indexName]
      }
    }
  }

  function setTimer() {
    // Turn on
    timer = setInterval(() => {
      if (i === iTimer) {
        // Turn off because nothing has been updated recently
        clearInterval(timer)
        timer = null
        i = iTimer = 0
      } else {
        iTimer = i
        pruneStatus()
        obv.set(indexesStatus)
      }
    }, EMIT_INTERVAL)
    if (timer.unref) timer.unref()
  }

  function batchUpdate(indexes, names) {
    const now = Date.now()
    for (const indexName of names) {
      indexesStatus[indexName] = indexes[indexName].offset
      indexesLastTime[indexName] = now
    }

    ++i
    if (!timer) {
      iTimer = i
      pruneStatus()
      obv.set(indexesStatus)
      setTimer()
    }
  }

  return {
    obv,
    batchUpdate,
  }
}
