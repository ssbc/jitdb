const helpers = require('./helpers');

function query(...cbs) {
  let res = cbs[0];
  for (let i = 1, n = cbs.length; i < n; i++) res = cbs[i](res);
  return res;
}

function fromDB(db) {
  return {
    meta: {db},
  };
}

function type(value) {
  return {
    type: 'EQUAL',
    data: {
      seek: helpers.seekType,
      value,
      indexType: 'type',
    },
  };
}

function author(value) {
  return {
    type: 'EQUAL',
    data: {
      seek: helpers.seekAuthor,
      value,
      indexType: 'author',
    },
  };
}

function channel(value) {
  return {
    type: 'EQUAL',
    data: {
      seek: helpers.seekChannel,
      value,
      indexType: 'channel',
    },
  };
}

function isRoot() {
  return {
    type: 'EQUAL',
    data: {
      seek: helpers.seekRoot,
      value: undefined,
      indexType: 'root',
    },
  };
}

function isPrivate() {
  return {
    type: 'EQUAL',
    data: {
      seek: helpers.seekPrivate,
      value: 'true',
      indexType: 'private',
    },
  };
}

function debug() {
  return (ops) => {
    console.log(
      'debug',
      JSON.stringify(
        ops,
        (key, val) => {
          if (key === 'db') return undefined;
          else return val;
        },
        2,
      ),
    );
    return ops;
  };
}

function moveMeta(orig, dest) {
  if (orig.meta) {
    dest.meta = orig.meta;
    delete orig.meta;
  }
}

function updateMeta(orig, key, value) {
  const res = Object.assign({}, orig);
  res.meta[key] = value;
  return res;
}

function extractMeta(orig) {
  const meta = orig.meta;
  delete orig.meta;
  return meta;
}

// FIXME: might or might not be correct logic, we need to unit tests all cases
function and(...rhs) {
  return (ops) => {
    const res = ops.type
      ? {
          type: 'AND',
          data: [ops, ...rhs],
        }
      : rhs.length > 1
      ? {
          type: 'AND',
          data: rhs,
        }
      : rhs[0];
    moveMeta(ops, res);
    return res;
  };
}

// FIXME: might or might not be correct logic, we need to unit tests all cases
function or(...rhs) {
  return (ops) => {
    const res = ops.type
      ? {
          type: 'OR',
          data: [ops, ...rhs],
        }
      : rhs.length > 1
      ? {
          type: 'OR',
          data: rhs,
        }
      : rhs[0];
    moveMeta(ops, res);
    return res;
  };
}

function ascending() {
  return (ops) => updateMeta(ops, 'reverse', true);
}

function startFrom(offset) {
  return (ops) => updateMeta(ops, 'offset', offset);
}

function paginate(pageSize) {
  return (ops) => updateMeta(ops, 'pageSize', pageSize);
}

function toCallback(cb) {
  return (ops) => {
    const meta = extractMeta(ops);
    if (meta.pageSize)
      meta.db.paginate(ops, meta.offset || 0, meta.pageSize, meta.reverse, cb);
    // FIXME: use `meta.offset || 0` here
    else meta.db.all(ops, cb);
  };
}

function toPromise() {
  return (ops) => {
    const meta = extractMeta(ops);
    return new Promise((resolve, reject) => {
      const cb = (err, data) => {
        if (err) reject(err);
        else resolve(data);
      };
      if (meta.pageSize)
        meta.db.paginate(
          ops,
          meta.offset || 0,
          meta.pageSize,
          meta.reverse,
          cb,
        );
      // FIXME: use `meta.offset || 0` here
      else meta.db.all(ops, cb);
    });
  };
}

function toPullStream() {
  return (ops) => {
    const meta = extractMeta(ops);
    let offset = meta.offset || 0;
    let total = Infinity;
    const limit = meta.pageSize || 1;
    return function readable(end, cb) {
      if (end) return cb(end);
      if (offset >= total) return cb(true);
      meta.db.paginate(ops, offset, limit, meta.reverse, (err, result) => {
        if (err) return cb(err);
        else {
          total = result.total;
          offset += limit;
          cb(null, !meta.pageSize ? result.data[0] : result.data);
        }
      });
    };
  };
}

// `async function*` supported in Node 10+ and browsers (except IE11)
function toAsyncIter() {
  return async function* (ops) {
    const meta = extractMeta(ops);
    let offset = meta.offset || 0;
    let total = Infinity;
    const limit = meta.pageSize || 1;
    while (offset < total) {
      yield await new Promise((resolve, reject) => {
        meta.db.paginate(ops, offset, limit, meta.reverse, (err, result) => {
          if (err) return reject(err);
          else {
            total = result.total;
            offset += limit;
            resolve(!meta.pageSize ? result.data[0] : result.data);
          }
        });
      });
    }
  };
}

module.exports = {
  fromDB,
  query,

  and,
  or,

  ascending,
  startFrom,
  paginate,
  toCallback,
  toPullStream,
  toPromise,
  toAsyncIter,

  debug,

  type,
  author,
  channel,
  isRoot,
  isPrivate,
};
