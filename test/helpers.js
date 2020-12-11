const bipf = require('bipf')

const bValue = Buffer.from('value')
const bVote = Buffer.from('vote')
const bLink = Buffer.from('link')
const bAuthor = Buffer.from('author')
const bContent = Buffer.from('content')
const bType = Buffer.from('type')
const bRoot = Buffer.from('root')
const bMeta = Buffer.from('meta')
const bAnimals = Buffer.from('animals')
const bWord = Buffer.from('word')
const bPrivate = Buffer.from('private')
const bChannel = Buffer.from('channel')

module.exports = {
  seekAuthor: function (buffer) {
    var p = 0 // note you pass in p!
    p = bipf.seekKey(buffer, p, bValue)

    if (~p) return bipf.seekKey(buffer, p, bAuthor)
  },

  seekVoteLink: function (buffer) {
    var p = 0 // note you pass in p!
    p = bipf.seekKey(buffer, p, bValue)
    if (!~p) return
    p = bipf.seekKey(buffer, p, bContent)
    if (!~p) return
    p = bipf.seekKey(buffer, p, bVote)
    if (!~p) return
    return bipf.seekKey(buffer, p, bLink)
  },

  seekType: function (buffer) {
    var p = 0 // note you pass in p!
    p = bipf.seekKey(buffer, p, bValue)

    if (~p) {
      p = bipf.seekKey(buffer, p, bContent)
      if (~p) return bipf.seekKey(buffer, p, bType)
    }
  },

  seekAnimals: function (buffer) {
    var p = 0 // note you pass in p!
    p = bipf.seekKey(buffer, p, bValue)
    if (!~p) return
    p = bipf.seekKey(buffer, p, bContent)
    if (!~p) return
    return bipf.seekKey(buffer, p, bAnimals)
  },

  pluckWord: function (buffer, start) {
    var p = start
    return bipf.seekKey(buffer, p, bWord)
  },

  seekRoot: function (buffer) {
    var p = 0 // note you pass in p!
    p = bipf.seekKey(buffer, p, bValue)

    if (~p) {
      p = bipf.seekKey(buffer, p, bContent)
      if (~p) return bipf.seekKey(buffer, p, bRoot)
    }
  },

  seekPrivate: function (buffer) {
    var p = 0 // note you pass in p!
    p = bipf.seekKey(buffer, p, bValue)

    if (~p) {
      p = bipf.seekKey(buffer, p, bMeta)
      if (~p) return bipf.seekKey(buffer, p, bPrivate)
    }
  },

  seekChannel: function (buffer) {
    var p = 0 // note you pass in p!
    p = bipf.seekKey(buffer, p, bValue)

    if (~p) {
      p = bipf.seekKey(buffer, p, bContent)
      if (~p) return bipf.seekKey(buffer, p, bChannel)
    }
  },
}
