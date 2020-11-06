const helpers = require("./helpers")

function query(...cbs) {
  let res = cbs[0];
  for (let i = 1, n = cbs.length; i < n; i++) res = cbs[i](res);
  return res;
}

function fromDB(db) {
  return {
    meta: {
      db
    }
  }
}

function type(value) {
  return {
    type: 'EQUAL',
    data: {
      seek: helpers.seekType,
      value,
      indexType: "type"
    }
  }
}

function author(value) {
  return {
    type: 'EQUAL',
    data: {
      seek: helpers.seekAuthor,
      value,
      indexType: "author"
    }
  }
}

function debug() {
  return (ops) => {
    console.log("debug", JSON.stringify(ops, (key, val) => {
      if (key == 'meta') return undefined
      else return val
    }, 2))
    return ops
  }
}

function and(...rhs) {
  return (ops) => {
    const res = ops.type ?
          {
            type: 'AND',
            data: [ops, ...rhs]
          } : rhs.length > 1 ?
          {
            type: 'AND',
            data: rhs
          } :
          rhs[0]

    if (ops.meta) {
      res.meta = ops.meta
      delete ops.meta
    }

    return res
  }
}

function or(...rhs) {
  return (ops) => {
    const res = ops.type ?
          {
            type: 'AND',
            data: [ops, rhs.length > 1 ? {
              type: 'OR',
              data: rhs
            } : rhs[0]]
          } : rhs

    if (ops.meta) {
      res.meta = ops.meta
      delete ops.meta
    }

    return res
  }
}

function ascending() {
  return ops => {
    const res = Object.assign({}, ops)
    res.meta.reverse = true
    return res
  }
}

function paginate(pageSize) {
  return ops => {
    const res = Object.assign({}, ops)
    res.meta.pageSize = pageSize
    return res
  }
}

function toCallback(cb) {
  return (ops) => {
    const meta = ops.meta
    delete ops.meta
    if (meta.pageSize)
      meta.db.paginate(ops, 0, meta.pageSize, meta.reverse, cb)
    else
      meta.db.all(ops, cb)
  }
}

module.exports = {
  fromDB,
  query,

  and,
  or,

  ascending,
  paginate,
  toCallback,
  
  debug,
  
  type,
  author
}
