const helpers = require("./helpers")

Paginator = function(query, offset, limit, total) {
  function getData(paginator, cb) {
    query.paginate(paginator.offset, paginator.limit, (err, result) => {
      if (err) return cb(err)
      else cb(null, result)
    })
  }
  
  let self = {
    query,
    offset,
    limit,
    total,
    next: function(cb) {
      if (self.offset >= self.total)
        cb(null, [])
      else {
        getData(self, (err, data) => {
          self.offset += self.limit
          cb(err, data)
        })
      }
    },
    prev: function(cb) {
      self.offset -= self.limit
      if (self.offset <= 0)
        cb(null, [])
      else
        getData(self, cb)
    }
  }

  return self
}

module.exports = function(jitdb, operation) {
  let self = {
    op: operation,
    reverse: false,
    // FIXME: support query instead of op
    AND: function(rhs) {
      if (Array.isArray(rhs))
        rhs = module.exports.AND(rhs)

      self.op = {
        type: 'AND',
        data: [self.op, rhs]
      }

      return self
    },
    OR: function(rhs) {
      if (Array.isArray(rhs))
        rhs = module.exports.OR(rhs)

      self.op = {
        type: 'OR',
        data: [self.op, rhs]
      }

      return self
    },
    ascending: function() {
      reverse = true
      return self
    },
    paginate: function(offset, limit, cb) {
      jitdb.paginate(self.op, offset, limit, self.reverse, (err, result) => {
        if (err) return cb(err)

        cb(null, result.data, Paginator(self, offset += limit, limit, result.total))
      })
    },
    all: function(cb) {
      jitdb.all(self.op, cb)
    },
  }

  return self
}

module.exports.AND = function(ops) {
  if (Array.isArray(ops))
  {
    let op = ops[0]
    
    ops.slice(1).forEach(r => {
      op = {
        type: 'AND',
        data: [op, r]
      }
    })

    return op
  }
}

module.exports.OR = function(ops) {
  if (Array.isArray(ops))
  {
    let op = ops[0]
    
    ops.slice(1).forEach(r => {
      op = {
        type: 'OR',
        data: [op, r]
      }
    })

    return op
  }
}

module.exports.type = function(value) {
  return {
    type: 'EQUAL',
    data: {
      seek: helpers.seekType,
      value,
      indexType: "type"
    }
  }
}

module.exports.author = function(value) {
  return {
    type: 'EQUAL',
    data: {
      seek: helpers.seekAuthor,
      value,
      indexType: "author"
    }
  }
}

module.exports.channel = function(value) {
  return {
    type: 'EQUAL',
    data: {
      seek: helpers.seekChannel,
      value,
      indexType: "channel"
    }
  }
}

module.exports.isRoot = function() {
  return {
    type: 'EQUAL',
    data: {
      seek: helpers.seekRoot,
      value: undefined,
      indexType: "root"
    }
  }
}

module.exports.isPrivate = function() {
  return {
    type: 'EQUAL',
    data: {
      seek: helpers.seekPrivate,
      value: "true",
      indexType: "private"
    }
  }
}
