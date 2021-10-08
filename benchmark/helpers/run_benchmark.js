// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

const prettyBytes = require('pretty-bytes')
const gc = require('expose-gc/function')
const nodemark = require('nodemark')

const prettyBytesOptions = {
  maximumFractionDigits: 2
}

const heapToString = function() {
  const formattedMean = prettyBytes(this.mean, prettyBytesOptions)
  const [mean, units] = formattedMean.split(' ')
  const formattedError = `${Math.round(Math.abs(this.error *  mean) * 100)/100} ${units}`
  return `${formattedMean} \xb1 ${formattedError}`
}

const statsToString = function() {
  const opsMs = this.ops.milliseconds(2)
  const opsError = Math.round(this.ops.error * opsMs * 100) / 100
  return `| ${this.name} | ${opsMs}ms \xb1 ${opsError}ms | ${this.heap} | ${this.ops.count} |\n`
}

function runBenchmark(benchmarkName, benchmarkFn, setupFn, callback) {
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
    cb()
  }
  function onStart(cb) {
    samples = []
    gc()
    cb()
  }
  function getTestStats(name, ops) {
    // Remove part before v8 optimizations, mimicking nodemark
    samples = samples.slice(samples.length - ops.count)
    let sum = 0
    let sumSquaredValues = 0
    for (const val of samples) {
      sum += val
      sumSquaredValues += val * val
    }
    const count = samples.length;
    const sumOfSquares = sumSquaredValues - sum * sum / count;
    const standardDeviation = Math.sqrt(sumOfSquares / (count - 1));
    const criticalValue = 2 / Math.sqrt(count);
    const marginOfError = criticalValue * Math.sqrt(standardDeviation * standardDeviation / count);
    const mean = sum / count;
    const error = (marginOfError / Math.abs(mean));
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
          else setupFn(function(err3) {
            if (err3) cb(err3)
            else {
              gc()
              cb()
            }
          })
        })
      },
      Number(process.env.BENCHMARK_DURATION_MS || 3000)
    ).then(result => {
      callback(null, getTestStats(benchmarkName, result))
    }).catch(e => {
      callback(e)
    })
  })
}

module.exports = runBenchmark