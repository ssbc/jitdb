// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

const bipf = require('bipf')

const B_KEY = Buffer.from('key')
const B_VALUE = Buffer.from('value')
const B_VOTE = Buffer.from('vote')
const B_LINK = Buffer.from('link')
const B_AUTHOR = Buffer.from('author')
const B_CONTENT = Buffer.from('content')
const B_TYPE = Buffer.from('type')
const B_ROOT = Buffer.from('root')
const B_META = Buffer.from('meta')
const B_ANIMALS = Buffer.from('animals')
const B_WORD = Buffer.from('word')
const B_PRIVATE = Buffer.from('private')
const B_CHANNEL = Buffer.from('channel')

module.exports = {
  seekKey: function (buffer) {
    var p = 0 // note you pass in p!
    return bipf.seekKey(buffer, p, B_KEY)
  },

  seekAuthor: function (buffer) {
    var p = 0 // note you pass in p!
    p = bipf.seekKey(buffer, p, B_VALUE)

    if (~p) return bipf.seekKey(buffer, p, B_AUTHOR)
  },

  seekVoteLink: function (buffer) {
    var p = 0 // note you pass in p!
    p = bipf.seekKey(buffer, p, B_VALUE)
    if (!~p) return
    p = bipf.seekKey(buffer, p, B_CONTENT)
    if (!~p) return
    p = bipf.seekKey(buffer, p, B_VOTE)
    if (!~p) return
    return bipf.seekKey(buffer, p, B_LINK)
  },

  seekType: function (buffer) {
    var p = 0 // note you pass in p!
    p = bipf.seekKey(buffer, p, B_VALUE)

    if (~p) {
      p = bipf.seekKey(buffer, p, B_CONTENT)
      if (~p) return bipf.seekKey(buffer, p, B_TYPE)
    }
  },

  seekAnimals: function (buffer) {
    var p = 0 // note you pass in p!
    p = bipf.seekKey(buffer, p, B_VALUE)
    if (!~p) return
    p = bipf.seekKey(buffer, p, B_CONTENT)
    if (!~p) return
    return bipf.seekKey(buffer, p, B_ANIMALS)
  },

  seekValue: function (buffer) {
    var p = 0 // note you pass in p!
    p = bipf.seekKey(buffer, p, B_VALUE)
    if (!~p) return
    p = bipf.seekKey(buffer, p, B_CONTENT)
    if (!~p) return
    return bipf.seekKey(buffer, p, B_VALUE)
  },

  pluckWord: function (buffer, start) {
    var p = start
    return bipf.seekKey(buffer, p, B_WORD)
  },

  seekRoot: function (buffer) {
    var p = 0 // note you pass in p!
    p = bipf.seekKey(buffer, p, B_VALUE)

    if (~p) {
      p = bipf.seekKey(buffer, p, B_CONTENT)
      if (~p) return bipf.seekKey(buffer, p, B_ROOT)
    }
  },

  seekPrivate: function (buffer) {
    var p = 0 // note you pass in p!
    p = bipf.seekKey(buffer, p, B_VALUE)

    if (~p) {
      p = bipf.seekKey(buffer, p, B_META)
      if (~p) return bipf.seekKey(buffer, p, B_PRIVATE)
    }
  },

  seekChannel: function (buffer) {
    var p = 0 // note you pass in p!
    p = bipf.seekKey(buffer, p, B_VALUE)

    if (~p) {
      p = bipf.seekKey(buffer, p, B_CONTENT)
      if (~p) return bipf.seekKey(buffer, p, B_CHANNEL)
    }
  },
}
