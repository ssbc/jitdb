const bipf = require('bipf')

module.exports = {
  seekAuthor: function(buffer) {
    var p = 0 // note you pass in p!
    p = bipf.seekKey(buffer, p, 'value')

    if (~p)
      return bipf.seekKey(buffer, p, 'author')
  },

  seekType: function(buffer) {
    var p = 0 // note you pass in p!
    p = bipf.seekKey(buffer, p, 'value')

    if (~p) {
      p = bipf.seekKey(buffer, p, 'content')
      if (~p)
        return bipf.seekKey(buffer, p, 'type')
    }
  },

  seekRoot: function(buffer) {
    var p = 0 // note you pass in p!
    p = bipf.seekKey(buffer, p, 'value')

    if (~p) {
      p = bipf.seekKey(buffer, p, 'content')
      if (~p)
        return bipf.seekKey(buffer, p, 'root')
    }
  },

  seekPrivate: function(buffer) {
    var p = 0 // note you pass in p!
    p = bipf.seekKey(buffer, p, 'value')

    if (~p) {
      p = bipf.seekKey(buffer, p, 'meta')
      if (~p)
        return bipf.seekKey(buffer, p, 'private')
    }
  },

  seekChannel: function(buffer) {
    var p = 0 // note you pass in p!
    p = bipf.seekKey(buffer, p, 'value')

    if (~p) {
      p = bipf.seekKey(buffer, p, 'content')
      if (~p)
        return bipf.seekKey(buffer, p, 'channel')
    }
  }
}
