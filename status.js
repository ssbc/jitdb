// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const Obv = require('obz')

module.exports = function Status() {
  let indexesStatus = {}
  const obv = Obv()
  obv.set(indexesStatus)
  const EMIT_INTERVAL = 1000 // ms
  let i = 0
  let iTimer = 0
  let timer = null

  function reset() {
    indexesStatus = {}
    if (timer) {
      clearInterval(timer)
      timer = null
      i = iTimer = 0
    }
    obv.set(indexesStatus)
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
        obv.set(indexesStatus)
      }
    }, EMIT_INTERVAL)
    if (timer.unref) timer.unref()
  }

  function batchUpdate(indexes, names) {
    for (const indexName of names) {
      const previous = indexesStatus[indexName] || -Infinity
      if (indexes[indexName].offset > previous) {
        indexesStatus[indexName] = indexes[indexName].offset
      }
    }

    ++i
    if (!timer) {
      iTimer = i
      obv.set(indexesStatus)
      setTimer()
    }
  }

  return {
    obv,
    reset,
    batchUpdate,
  }
}
