const bipf = require('bipf')

const bValue = Buffer.from('value')
const bAuthor = Buffer.from('author')
const bContent = Buffer.from('content')
const bType = Buffer.from('type')
const bRoot = Buffer.from('root')
const bMeta = Buffer.from('meta')
const bPrivate = Buffer.from('private')
const bChannel = Buffer.from('channel')

module.exports = {
  seekAuthor: function(buffer) {
    var p = 0 // note you pass in p!
    p = bipf.seekKey(buffer, p, bValue)

    if (~p)
      return bipf.seekKey(buffer, p, bAuthor)
  },

  // FIXME: move to somewhere opinionated, because it leans on msg conventions
  seekType: function(buffer) {
    var p = 0 // note you pass in p!
    p = bipf.seekKey(buffer, p, bValue)

    if (~p) {
      p = bipf.seekKey(buffer, p, bContent)
      if (~p)
        return bipf.seekKey(buffer, p, bType)
    }
  },

  // FIXME: move to somewhere opinionated, because it leans on msg conventions
  seekRoot: function(buffer) {
    var p = 0 // note you pass in p!
    p = bipf.seekKey(buffer, p, bValue)

    if (~p) {
      p = bipf.seekKey(buffer, p, bContent)
      if (~p)
        return bipf.seekKey(buffer, p, bRoot)
    }
  },

  // FIXME: move to somewhere opinionated, because it leans on msg conventions
  seekPrivate: function(buffer) {
    var p = 0 // note you pass in p!
    p = bipf.seekKey(buffer, p, bValue)

    if (~p) {
      p = bipf.seekKey(buffer, p, bMeta)
      if (~p)
        return bipf.seekKey(buffer, p, bPrivate)
    }
  },

  // FIXME: move to somewhere opinionated, because it leans on msg conventions
  seekChannel: function(buffer) {
    var p = 0 // note you pass in p!
    p = bipf.seekKey(buffer, p, bValue)

    if (~p) {
      p = bipf.seekKey(buffer, p, bContent)
      if (~p)
        return bipf.seekKey(buffer, p, bChannel)
    }
  }
}
