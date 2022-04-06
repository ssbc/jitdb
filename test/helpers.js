// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

const bipf = require('bipf')

const B_KEY = Buffer.from('key')
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
  toBipf(value) {
    return bipf.allocAndEncode(value)
  },

  seekKey(buffer) {
    return bipf.seekKey(buffer, 0, B_KEY)
  },

  seekAuthor(buffer, start, pValue) {
    if (pValue < 0) return -1
    return bipf.seekKey(buffer, pValue, B_AUTHOR)
  },

  seekVoteLink(buffer, start, pValue) {
    if (pValue < 0) return -1
    const pValueContent = bipf.seekKey(buffer, pValue, B_CONTENT)
    if (pValueContent < 0) return -1
    const pValueContentVote = bipf.seekKey(buffer, pValueContent, B_VOTE)
    if (pValueContentVote < 0) return -1
    return bipf.seekKey(buffer, pValueContentVote, B_LINK)
  },

  seekType(buffer, start, pValue) {
    if (pValue < 0) return -1
    const pValueContent = bipf.seekKey(buffer, pValue, B_CONTENT)
    if (pValueContent < 0) return -1
    return bipf.seekKey(buffer, pValueContent, B_TYPE)
  },

  seekAnimals(buffer, start, pValue) {
    if (pValue < 0) return -1
    const pValueContent = bipf.seekKey(buffer, pValue, B_CONTENT)
    if (pValueContent < 0) return -1
    return bipf.seekKey(buffer, pValueContent, B_ANIMALS)
  },

  pluckWord(buffer, start) {
    return bipf.seekKey(buffer, start, B_WORD)
  },

  seekRoot(buffer, start, pValue) {
    if (pValue < 0) return -1
    const pValueContent = bipf.seekKey(buffer, pValue, B_CONTENT)
    if (pValueContent < 0) return -1
    return bipf.seekKey(buffer, pValueContent, B_ROOT)
  },

  seekPrivate(buffer, start, pValue) {
    if (pValue < 0) return -1
    const pValueMeta = bipf.seekKey(buffer, pValue, B_META)
    if (pValueMeta < 0) return -1
    return bipf.seekKey(buffer, pValueMeta, B_PRIVATE)
  },

  seekChannel(buffer, start, pValue) {
    if (pValue < 0) return -1
    const pValueContent = bipf.seekKey(buffer, pValue, B_CONTENT)
    if (pValueContent < 0) return -1
    return bipf.seekKey(buffer, pValueContent, B_CHANNEL)
  },
}
