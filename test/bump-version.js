// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

const validate = require('ssb-validate')
const ssbKeys = require('ssb-keys')
const path = require('path')
const { prepareAndRunTest, addMsg, helpers } = require('./common')()
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')

const dir = '/tmp/jitdb-bump-version'
rimraf.sync(dir)
mkdirp.sync(dir)

var keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

const msg1 = { type: 'post', text: 'Testing post 1!' }
const msg2 = { type: 'contact', text: 'Testing contact!' }
const msg3 = { type: 'post', text: 'Testing post 2!' }

let state = validate.initial()
state = validate.appendNew(state, null, keys, msg1, Date.now())
state = validate.appendNew(state, null, keys, msg2, Date.now() + 1)
state = validate.appendNew(state, null, keys, msg3, Date.now() + 2)

prepareAndRunTest('Bitvector index version bumped', dir, (t, db, raf) => {
  const msg = { type: 'post', text: 'Testing 1' }
  let state = validate.initial()
  state = validate.appendNew(state, null, keys, msg, Date.now())

  const postQuery = {
    type: 'EQUAL',
    data: {
      seek: helpers.seekType,
      value: helpers.toBipf('post'),
      indexType: 'type',
      indexName: 'type_post',
    },
  }

  addMsg(state.queue[0].value, raf, (err, msg) => {
    db.all(postQuery, 0, false, false, (err, results) => {
      t.error(err)
      t.equal(results.length, 1)
      t.equal(results[0].value.content.text, 'Testing 1')

      const postQuery2 = {
        type: 'EQUAL',
        data: {
          seek: helpers.seekType,
          value: helpers.toBipf('post'),
          indexType: 'type',
          indexName: 'type_post',
          version: 2,
        },
      }

      db.all(postQuery2, 0, false, false, (err, results) => {
        t.error(err)
        t.equal(results.length, 1)
        t.equal(results[0].value.content.text, 'Testing 1')
        t.end()
      })
    })
  })
})

prepareAndRunTest('Prefix map index version bumped', dir, (t, db, raf) => {
  addMsg(state.queue[0].value, raf, (err, msg) => {
    addMsg(state.queue[1].value, raf, (err, msg) => {
      addMsg(state.queue[2].value, raf, (err, msg) => {
        const msgKey = state.queue[2].key
        const keyQuery = {
          type: 'EQUAL',
          data: {
            seek: helpers.seekKey,
            value: helpers.toBipf(msgKey),
            indexType: 'key',
            indexName: 'value_key',
            useMap: true,
            prefix: 32,
            prefixOffset: 1,
          },
        }

        db.all(keyQuery, 0, false, false, (err, results) => {
          t.equal(results.length, 1)
          t.equal(results[0].value.content.text, 'Testing post 2!')

          const keyQuery2 = {
            type: 'EQUAL',
            data: {
              seek: helpers.seekKey,
              value: helpers.toBipf(msgKey),
              indexType: 'key',
              indexName: 'value_key',
              useMap: true,
              prefix: 32,
              prefixOffset: 4,
              version: 2,
            },
          }

          db.all(keyQuery2, 0, false, false, (err, results) => {
            t.equal(results.length, 1)
            t.equal(results[0].value.content.text, 'Testing post 2!')
            t.end()
          })
        })
      })
    })
  })
})
