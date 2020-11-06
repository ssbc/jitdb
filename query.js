const helpers = require("./helpers")

module.exports.query = function(jitdb, operation) {
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

  let self = {
    op: operation,
    reverse: false,
    // FIXME: support query instead of op
    and: function(rhs) {
      if (Array.isArray(rhs))
        rhs = module.exports.and(rhs)

      // FIXME return copy instead of modifying
      self.op = {
        type: 'AND',
        data: [self.op, rhs]
      }

      return self
    },
    or: function(rhs) {
      if (Array.isArray(rhs))
        rhs = module.exports.or(rhs)

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

module.exports.filter = {
  and: function(ops) {
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
  },

  or: function(ops) {
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
  },

  type: function(value) {
    return {
      type: 'EQUAL',
      data: {
        seek: helpers.seekType,
        value,
        indexType: "type"
      }
    }
  },

  author: function(value) {
    return {
      type: 'EQUAL',
      data: {
        seek: helpers.seekAuthor,
        value,
        indexType: "author"
      }
    }
  },

  channel: function(value) {
    return {
      type: 'EQUAL',
      data: {
        seek: helpers.seekChannel,
        value,
        indexType: "channel"
      }
    }
  },

  isRoot: function() {
    return {
      type: 'EQUAL',
      data: {
        seek: helpers.seekRoot,
        value: undefined,
        indexType: "root"
      }
    }
  },

  isPrivate: function() {
    return {
      type: 'EQUAL',
      data: {
        seek: helpers.seekPrivate,
        value: "true",
        indexType: "private"
      }
    }
  }
}
