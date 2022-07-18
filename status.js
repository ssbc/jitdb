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
  const activeIndexNames = new Set()

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

  function update(indexes, names) {
    let changed = false
    for (const name of names) {
      const index = indexes.get(name)
      const previous = indexesStatus[name] || -Infinity
      if (index.offset > previous) {
        indexesStatus[name] = index.offset
        activeIndexNames.add(name)
        changed = true
      }
    }
    if (!changed) return

    ++i
    if (!timer) {
      iTimer = i
      obv.set(indexesStatus)
      setTimer()
    }
  }

  function done(names) {
    for (const name of names) activeIndexNames.delete(name)
    if (activeIndexNames.size === 0) {
      indexesStatus = {}

      ++i
      if (!timer) {
        iTimer = i
        obv.set(indexesStatus)
        setTimer()
      }
    }
  }

  return {
    obv,
    done,
    update,
  }
}
