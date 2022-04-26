// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const bipf = require('bipf')
const traverse = require('traverse')
const promisify = require('promisify-4loc')
const pull = require('pull-stream')
const multicb = require('multicb')
const pullAwaitable = require('pull-awaitable')
const cat = require('pull-cat')
const { safeFilename } = require('./files')

//#region Helper functions and util operators

function copyMeta(orig, dest) {
  if (orig.meta) {
    dest.meta = orig.meta
  }
}

function updateMeta(orig, key, value) {
  const res = Object.assign({}, orig)
  res.meta[key] = value
  return res
}

function extractMeta(orig) {
  const meta = orig.meta
  return meta
}

const seekFromDescCache = new Map()
function seekFromDesc(desc) {
  if (seekFromDescCache.has(desc)) {
    return seekFromDescCache.get(desc)
  }
  const keys = desc.split('.').map((str) => bipf.allocAndEncode(str))
  // The 2nd arg `start` is to support plucks too
  const fn = function (buffer, start = 0) {
    var p = start
    for (let key of keys) {
      p = bipf.seekKey2(buffer, p, key, 0)
      if (p < 0) return -1
    }
    return p
  }
  seekFromDescCache.set(desc, fn)
  return fn
}

function getIndexName(opts, indexType, valueName) {
  return safeFilename(
    opts.prefix
      ? opts.useMap
        ? indexType + '__map'
        : indexType
      : indexType + '_' + valueName
  )
}

function query(...cbs) {
  let res = cbs[0]
  for (let i = 1, n = cbs.length; i < n; i++) if (cbs[i]) res = cbs[i](res)
  return res
}

function debug() {
  return (ops) => {
    const meta = JSON.stringify(ops.meta, (key, val) =>
      key === 'jitdb' ? void 0 : val
    )
    console.log(
      'debug',
      JSON.stringify(
        ops,
        (key, val) => {
          if (key === 'meta') return void 0
          else if (key === 'task' && typeof val === 'function')
            return '[Function]'
          else if (key === 'value' && val.type === 'Buffer')
            return Buffer.from(val.data).toString()
          else return val
        },
        2
      ),
      meta === '{}' ? '' : 'meta: ' + meta
    )
    return ops
  }
}

//#endregion
//#region "Unit operators": they create objects that JITDB interprets

function slowEqual(seekDesc, target, opts) {
  opts = opts || {}
  const seek = seekFromDesc(seekDesc)
  const value = bipf.allocAndEncode(target)
  const valueName = !target ? '' : `${target}`
  const indexType = seekDesc.replace(/\./g, '_')
  const indexName = getIndexName(opts, indexType, valueName)
  return {
    type: 'EQUAL',
    data: {
      seek,
      value,
      indexType,
      indexName,
      useMap: opts.useMap,
      indexAll: opts.indexAll,
      prefix: opts.prefix,
      prefixOffset: opts.prefixOffset,
    },
  }
}

function equal(seek, target, opts) {
  opts = opts || {}
  if (!opts.indexType)
    throw new Error('equal() operator needs an indexType in the 3rd arg')
  const value = bipf.allocAndEncode(target)
  const valueName = !target ? '' : `${target}`
  const indexType = opts.indexType
  const indexName = getIndexName(opts, indexType, valueName)
  return {
    type: 'EQUAL',
    data: {
      seek,
      value,
      indexType,
      indexName,
      useMap: opts.useMap,
      indexAll: opts.indexAll,
      prefix: opts.prefix,
      prefixOffset: opts.prefixOffset,
    },
  }
}

function slowPredicate(seekDesc, fn, opts) {
  opts = opts || {}
  const seek = seekFromDesc(seekDesc)
  if (typeof fn !== 'function')
    throw new Error('predicate() needs a predicate function in the 2rd arg')
  const value = fn
  const indexType = seekDesc.replace(/\./g, '_')
  const name = opts.name || fn.name
  if (!name) throw new Error('predicate() needs opts.name')
  const indexName = safeFilename(indexType + '__pred_' + name)
  return {
    type: 'PREDICATE',
    data: {
      seek,
      value,
      indexType,
      indexName,
    },
  }
}

function predicate(seek, fn, opts) {
  opts = opts || {}
  if (!opts.indexType)
    throw new Error('predicate() operator needs an indexType in the 3rd arg')
  if (typeof fn !== 'function')
    throw new Error('predicate() needs a predicate function in the 2rd arg')
  const value = fn
  const indexType = opts.indexType
  const name = opts.name || fn.name
  if (!name) throw new Error('predicate() needs opts.name')
  const indexName = safeFilename(indexType + '__pred_' + name)
  return {
    type: 'PREDICATE',
    data: {
      seek,
      value,
      indexType,
      indexName,
    },
  }
}

function slowAbsent(seekDesc) {
  const seek = seekFromDesc(seekDesc)
  const indexType = seekDesc.replace(/\./g, '_')
  const indexName = safeFilename(indexType + '__absent')
  return {
    type: 'ABSENT',
    data: {
      seek,
      indexType,
      indexName,
    },
  }
}

function absent(seek, opts) {
  opts = opts || {}
  if (!opts.indexType)
    throw new Error('absent() operator needs an indexType in the 3rd arg')
  const indexType = opts.indexType
  const indexName = safeFilename(indexType + '__absent')
  return {
    type: 'ABSENT',
    data: {
      seek,
      indexType,
      indexName,
    },
  }
}

function slowIncludes(seekDesc, target, opts) {
  opts = opts || {}
  const seek = seekFromDesc(seekDesc)
  const value = bipf.allocAndEncode(target)
  if (!value) throw new Error('slowIncludes() 2nd arg needs to be truthy')
  const valueName = !target ? '' : `${target}`
  const indexType = seekDesc.replace(/\./g, '_')
  const indexName = safeFilename(indexType + '_' + valueName)
  const pluck =
    opts.pluck && typeof opts.pluck === 'string'
      ? seekFromDesc(opts.pluck)
      : opts.pluck
  return {
    type: 'INCLUDES',
    data: {
      seek,
      value,
      indexType,
      indexName,
      indexAll: opts.indexAll,
      pluck,
    },
  }
}

function includes(seek, target, opts) {
  opts = opts || {}
  if (!opts.indexType)
    throw new Error('includes() operator needs an indexType in the 3rd arg')
  const value = bipf.allocAndEncode(target)
  if (!value) throw new Error('includes() 2nd arg needs to be truthy')
  const valueName = !target ? '' : `${target}`
  const indexType = opts.indexType
  const indexName = safeFilename(indexType + '_' + valueName)
  return {
    type: 'INCLUDES',
    data: {
      seek,
      value,
      indexType,
      indexName,
      indexAll: opts.indexAll,
      pluck: opts.pluck,
    },
  }
}

function gt(value, indexName) {
  if (typeof value !== 'number') throw new Error('gt() needs a number arg')
  return {
    type: 'GT',
    data: {
      value,
      indexName,
    },
  }
}

function gte(value, indexName) {
  if (typeof value !== 'number') throw new Error('gte() needs a number arg')
  return {
    type: 'GTE',
    data: {
      value,
      indexName,
    },
  }
}

function lt(value, indexName) {
  if (typeof value !== 'number') throw new Error('lt() needs a number arg')
  return {
    type: 'LT',
    data: {
      value,
      indexName,
    },
  }
}

function lte(value, indexName) {
  if (typeof value !== 'number') throw new Error('lte() needs a number arg')
  return {
    type: 'LTE',
    data: {
      value,
      indexName,
    },
  }
}

function seqs(values) {
  return {
    type: 'SEQS',
    seqs: values,
  }
}

function liveSeqs(pullStream) {
  return {
    type: 'LIVESEQS',
    stream: pullStream,
  }
}

function offsets(values) {
  return {
    type: 'OFFSETS',
    offsets: values,
  }
}

function deferred(task) {
  return {
    type: 'DEFERRED',
    task,
  }
}

//#endregion
//#region "Combinator operators": they build composite operations

function not(ops) {
  return {
    type: 'NOT',
    data: [ops],
  }
}

function and(...args) {
  const validargs = args.filter((arg) => !!arg)
  if (validargs.length === 0) return {}
  if (validargs.length === 1) return validargs[0]
  return { type: 'AND', data: validargs }
}

function or(...args) {
  const validargs = args.filter((arg) => !!arg)
  if (validargs.length === 0) return {}
  if (validargs.length === 1) return validargs[0]
  return { type: 'OR', data: validargs }
}

function where(...args) {
  return (prevOp) => {
    if (args.length !== 1) throw new Error('where() accepts only one argument')
    const nextOp = args[0]
    if (!nextOp) return prevOp
    const res = prevOp.type ? { type: 'AND', data: [prevOp, nextOp] } : nextOp
    copyMeta(prevOp, res)
    return res
  }
}

//#endregion
//#region "Special operators": they only update meta

function fromDB(jitdb) {
  return {
    meta: { jitdb },
  }
}

function live(opts) {
  if (opts && opts.old) return (ops) => updateMeta(ops, 'live', 'liveAndOld')
  else return (ops) => updateMeta(ops, 'live', 'liveOnly')
}

function count() {
  return (ops) => updateMeta(ops, 'count', true)
}

function descending() {
  return (ops) => updateMeta(ops, 'descending', true)
}

function sortByArrival() {
  return (ops) => updateMeta(ops, 'sortBy', 'arrival')
}

function startFrom(seq) {
  return (ops) => updateMeta(ops, 'seq', seq)
}

function paginate(pageSize) {
  return (ops) => updateMeta(ops, 'pageSize', pageSize)
}

function batch(batchSize) {
  return (ops) => updateMeta(ops, 'batchSize', batchSize)
}

function asOffsets() {
  return (ops) => updateMeta(ops, 'asOffsets', true)
}

//#endregion
//#region "Consumer operators": they execute the query tree

function executeDeferredOps(ops, meta) {
  // Collect all deferred tasks and their object-traversal paths
  const allDeferred = []
  traverse.forEach(ops, function (val) {
    if (!val) return
    // this.block() means don't traverse inside these, they won't have DEFERRED
    if (this.key === 'meta' && val.jitdb) return this.block()
    if (val.type === 'DEFERRED' && val.task) {
      allDeferred.push([this.path, val.task])
    }
    if (!(Array.isArray(val) || val.type === 'AND' || val.type === 'OR')) {
      this.block()
    }
  })
  if (allDeferred.length === 0) return pull.values([ops])

  // State needed throughout the execution of the `readable`
  const done = multicb({ pluck: 1 })
  let completed = false
  const abortListeners = []
  function addAbortListener(listener) {
    abortListeners.push(listener)
  }

  return function readable(end, cb) {
    if (end) {
      while (abortListeners.length) abortListeners.shift()()
      cb(end)
      return
    }
    if (completed) {
      cb(true)
      return
    }

    // Execute all deferred tasks and collect the results (and the paths)
    for (const [path, task] of allDeferred) {
      const taskCB = done()
      task(
        meta,
        (err, result) => {
          if (err) taskCB(err)
          else if (!result) taskCB(null, [path, {}])
          else if (typeof result === 'function') taskCB(null, [path, result()])
          else taskCB(null, [path, result])
        },
        addAbortListener
      )
    }

    // When all tasks are done...
    done((err, results) => {
      if (err) return cb(err)

      // Replace/mutate all deferreds with their respective results
      for (const [path, result] of results) {
        result.meta = meta
        if (path.length === 0) ops = result
        else traverse.set(ops, path, result)
      }
      completed = true
      cb(null, ops)
    })
  }
}

function toCallback(cb) {
  return (rawOps) => {
    const meta = extractMeta(rawOps)
    const readable = executeDeferredOps(rawOps, meta)
    readable(null, (end, ops) => {
      if (end) return cb(end)

      const seq = meta.seq || 0
      const { pageSize, descending, asOffsets, sortBy } = meta
      if (meta.count) meta.jitdb.count(ops, seq, descending, cb)
      else if (pageSize)
        meta.jitdb.paginate(
          ops,
          seq,
          pageSize,
          descending,
          asOffsets,
          sortBy,
          cb
        )
      else meta.jitdb.all(ops, seq, descending, asOffsets, sortBy, cb)
    })
  }
}

function toPromise() {
  return (rawOps) => {
    return promisify((cb) => toCallback(cb)(rawOps))()
  }
}

function toPullStream() {
  return (rawOps) => {
    const meta = extractMeta(rawOps)

    function paginateStream(ops) {
      let seq = meta.seq || 0
      let total = Infinity
      const limit = meta.pageSize || meta.batchSize || 20
      let shouldEnd = false
      function readable(end, cb) {
        if (end) return cb(end)
        if (seq >= total || shouldEnd) return cb(true)
        if (meta.count) {
          shouldEnd = true
          meta.jitdb.count(ops, seq, meta.descending, cb)
        } else {
          meta.jitdb.paginate(
            ops,
            seq,
            limit,
            meta.descending,
            meta.asOffsets,
            meta.sortBy,
            (err, answer) => {
              if (err) return cb(err)
              else if (answer.total === 0) cb(true)
              else {
                total = answer.total
                seq += limit
                cb(null, answer.results)
              }
            }
          )
        }
      }
      if (meta.pageSize || meta.count) {
        return readable
      } else {
        // Flatten the "pages" (arrays) into individual messages
        return pull(readable, pull.map(pull.values), pull.flatten())
      }
    }

    return pull(
      executeDeferredOps(rawOps, meta),
      pull.map((ops) => {
        if (meta.live === 'liveOnly') return meta.jitdb.live(ops)
        else if (meta.live === 'liveAndOld')
          return cat([paginateStream(ops), meta.jitdb.live(ops)])
        else return paginateStream(ops)
      }),
      pull.flatten()
    )
  }
}

// `async function*` supported in Node 10+ and browsers (except IE11)
function toAsyncIter() {
  return async function* (rawOps) {
    const ps = toPullStream()(rawOps)
    for await (let x of pullAwaitable(ps)) yield x
  }
}

//#endregion

module.exports = {
  fromDB,
  query,

  live,
  slowEqual,
  equal,
  slowPredicate,
  predicate,
  slowAbsent,
  absent,
  slowIncludes,
  includes,
  where,
  not,
  gt,
  gte,
  lt,
  lte,
  and,
  or,
  deferred,
  liveSeqs,

  seqs,
  offsets,

  descending,
  sortByArrival,
  count,
  startFrom,
  paginate,
  batch,
  asOffsets,
  toCallback,
  toPullStream,
  toPromise,
  toAsyncIter,

  debug,
}
