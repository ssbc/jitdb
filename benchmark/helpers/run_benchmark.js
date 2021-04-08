const prettyBytes = require('pretty-bytes')
const gc = require('expose-gc/function')
const nodemark = require('nodemark')

const prettyBytesOptions = {
  maximumFractionDigits: 2
}

const heapToString = function() {
  const formattedMean = prettyBytes(this.mean, prettyBytesOptions)
  const [mean, units] = formattedMean.split(' ')
  const formattedStandardDeviation = `${Math.round(Math.abs(this.error *  mean) * 100)/100} ${units}`
  return `${formattedMean} \xb1 ${formattedStandardDeviation}`
}

const statsToString = function() {
  const opsMs = this.ops.milliseconds(2)
  const opsError = Math.round(this.ops.error * opsMs * 100) / 100
  return `| ${this.name} | ${opsMs}ms \xb1 ${opsError}ms | ${this.heap} | ${this.ops.count} |\n`
}

function runBenchmark(benchmarkName, benchmarkFn, setupFn, callback, notCountedFn) {
  let samples
  let oldMemory
  function calcMemUsage() {
    const newMemory = process.memoryUsage().heapUsed
    if (oldMemory === void 0) {
      oldMemory = newMemory
    } else {
      samples.push(newMemory - oldMemory)
      oldMemory = newMemory
    }
  }

  function onCycle(cb) {
    calcMemUsage()
    if (notCountedFn) {
      notCountedFn(function(err) {
        if (err) cb(err)
        else gc()
      })
    } else {
      gc()
      cb()
    }
  }
  function onStart(cb) {
    samples = []
    gc()
    cb()
  }
  function getTestStats(name, ops) {
    // Remove part before v8 optimizations, mimicking nodemark
    samples = samples.slice(samples.length - ops.count)
    const mean = (samples.reduce((a,s) => a+s, 0) / samples.length) | 0
    const error = (Math.sqrt(
      (samples.reduce((agg, diff) => agg + (diff - mean) * (diff - mean), 0) / (samples.length - 1)) | 0
    ) | 0) / Math.abs(mean)
    const heap = {
      mean,
      error,
      toString: heapToString
    }
    return {
      name,
      ops,
      heap,
      toString: statsToString
    }
  }

  onStart(function(err) {
    if (err) return callback(err)
    nodemark(
      benchmarkFn,
      (cb) => {
        onCycle(function(err2) {
          if (err2) return cb(err2)
          else setupFn(cb)
        })
      }
    ).then(result => {
      callback(null, getTestStats(benchmarkName, result))
    }).catch(e => {
      callback(e)
    })
  })
}

module.exports = runBenchmark