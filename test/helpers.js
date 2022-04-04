// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

const bipf = require('bipf')

module.exports = {
  toBipf(value) {
    return bipf.allocAndEncode(value)
  },

  seekKey(buffer) {
    return bipf.seekKey(buffer, 0, 'key')
  },

  seekAuthor(buffer) {
    const pValue = bipf.seekKeyCached(buffer, 0, 'value')
    if (pValue < 0) return
    return bipf.seekKey(buffer, pValue, 'author')
  },

  seekVoteLink(buffer) {
    const pValue = bipf.seekKeyCached(buffer, 0, 'value')
    if (pValue < 0) return
    const pValueContent = bipf.seekKeyCached(buffer, pValue, 'content')
    if (pValueContent < 0) return
    const pValueContentVote = bipf.seekKey(buffer, pValueContent, 'vote')
    if (pValueContentVote < 0) return
    return bipf.seekKey(buffer, pValueContentVote, 'link')
  },

  seekType(buffer) {
    const pValue = bipf.seekKeyCached(buffer, 0, 'value')
    if (pValue < 0) return
    const pValueContent = bipf.seekKeyCached(buffer, pValue, 'content')
    if (pValueContent < 0) return
    return bipf.seekKey(buffer, pValueContent, 'type')
  },

  seekAnimals(buffer) {
    const pValue = bipf.seekKeyCached(buffer, 0, 'value')
    if (pValue < 0) return
    const pValueContent = bipf.seekKeyCached(buffer, pValue, 'content')
    if (pValueContent < 0) return
    return bipf.seekKey(buffer, pValueContent, 'animals')
  },

  pluckWord(buffer, start) {
    return bipf.seekKey(buffer, start, 'word')
  },

  seekRoot(buffer) {
    const pValue = bipf.seekKeyCached(buffer, 0, 'value')
    if (pValue < 0) return
    const pValueContent = bipf.seekKeyCached(buffer, pValue, 'content')
    if (pValueContent < 0) return
    return bipf.seekKey(buffer, pValueContent, 'root')
  },

  seekPrivate(buffer) {
    const pValue = bipf.seekKeyCached(buffer, 0, 'value')
    if (pValue < 0) return
    const pValueMeta = bipf.seekKeyCached(buffer, pValue, 'meta')
    if (pValueMeta < 0) return
    return bipf.seekKey(buffer, pValueMeta, 'private')
  },

  seekChannel(buffer) {
    const pValue = bipf.seekKeyCached(buffer, 0, 'value')
    if (pValue < 0) return
    const pValueContent = bipf.seekKeyCached(buffer, pValue, 'content')
    if (pValueContent < 0) return
    return bipf.seekKey(buffer, pValueContent, 'channel')
  },
}
