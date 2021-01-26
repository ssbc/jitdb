const Obv = require('obz')

module.exports = function Status() {
  const indexesStatus = {}
  const obv = Obv()
  obv.set(indexesStatus)
  let i = 0
  let iTimer = 0
  let timer = null

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
        obv.set(indexesStatus)
      }
    }, 1000)
    if (timer.unref) timer.unref()
  }

  function maybeEmit() {
    ++i
    if (!timer) {
      iTimer = i
      obv.set(indexesStatus)
      setTimer()
    }
  }

  function update(indexName, offset) {
    indexesStatus[indexName] = offset
    maybeEmit()
  }

  function batchUpdate(indexes, names) {
    for (const indexName of names) {
      indexesStatus[indexName] = indexes[indexName].offset
    }
    maybeEmit()
  }

  return {
    obv,
    update,
    batchUpdate,
  }
}
