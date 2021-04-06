const prettyBytes = require('pretty-bytes');
const gc = require('expose-gc/function');
const nodemark = require('nodemark');

function runBenchmark(benchmarkName, benchmarkFn, setupFn, callback, notCountedFn) {
  let countMem = 0;
  let streamingTotalMem = 0;
  let streamingMeanMem = 0;
  let streamingMaxMem = 0;
  let streamingMinMem = Number.MAX_VALUE;
  let streamingVarianceTotal = 0;
  let oldMemory;
  function calcMemUsage() {
    const newMemory = process.memoryUsage().heapUsed;
    if (countMem === 0) {
      oldMemory = newMemory;
      countMem++;
    } else {
      const memDiff = newMemory - oldMemory;
      streamingMaxMem = Math.max(streamingMaxMem, memDiff);
      streamingMinMem = Math.min(streamingMinMem, memDiff);
      streamingTotalMem += memDiff;
      const newMean = (streamingTotalMem / countMem) | 0;
      streamingVarianceTotal += (memDiff - newMean) * (memDiff - streamingMeanMem);
      streamingMeanMem = newMean;
      countMem++;
      oldMemory = newMemory;
    }
  }

  function onCycle(cb) {
    calcMemUsage();
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
    countMem = 0;
    streamingTotalMem = 0;
    streamingMeanMem = 0;
    streamingMaxMem = 0;
    streamingMinMem = Number.MAX_VALUE;
    streamingVarianceTotal = 0;
    gc();
    cb()
  }
  function getFormattedResult(name, result) {
    const heapChange = `max: ${prettyBytes(streamingMaxMem)
    }, min:${prettyBytes(streamingMinMem)
    }, mean:${prettyBytes(streamingMeanMem)
    }, std dev:${prettyBytes(Math.sqrt((streamingVarianceTotal / (countMem - 1)) | 0) | 0)}`
    return `| ${name} | ${result} | ${heapChange} |\n`
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
      callback(null, getFormattedResult(benchmarkName, result))
    }).catch(e => {
      callback(e)
    })
  })
}

module.exports = runBenchmark;