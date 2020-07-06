const test = require('tape')
const FlumeLog = require('flumelog-aligned-offset')
const validate = require('ssb-validate')
const ssbKeys = require('ssb-keys')
const path = require('path')
const Obv = require('obv')
const bipf = require('bipf')
const hash = require('ssb-keys/util').hash

var state = validate.initial()

const dir = '/tmp/jitdb-add'
require('rimraf').sync(dir)

var keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))
var keys2 = ssbKeys.loadOrCreateSync(path.join(dir, 'secret2'))

var raf = FlumeLog(path.join(dir, "log"), {block: 64*1024})
raf.since = Obv()
raf.onWrite = raf.since.set

function getId(msg) {
  return '%'+hash(JSON.stringify(msg, null, 2))
}

function addMsg(msg, cb) {
  var data = {
    key: getId(msg),
    value: msg,
    timestamp: Date.now()
  }
  var b = Buffer.alloc(bipf.encodingLength(data))
  bipf.encode(data, b, 0)
  raf.append(b, false, function (err) {
    if (err) cb(err)
    else cb(null, data)
  })
}

var db = require('../index')(raf, path.join(dir, "indexes"))
db.onReady(() => {
  test('Base', t => {
    const msg = { type: 'post', text: 'Testing!' }
    state = validate.appendNew(state, null, keys, msg, Date.now())
    state = validate.appendNew(state, null, keys2, msg, Date.now())

    addMsg(state.queue[0].value, (err, msg1) => {
      addMsg(state.queue[1].value, (err, msg2) => {
        const typeQuery = {
          type: 'EQUAL',
          data: {
            seek: db.seekType,
            value: Buffer.from('post'),
            indexType: "type"
          }
        }
        db.query(typeQuery, 10, (err, results) => {
          t.equal(results.length, 2)

          // rerun on created index
          db.query(typeQuery, 10, (err, results) => {
            t.equal(results.length, 2)

            const authorQuery = {
              type: 'EQUAL',
              data: {
                seek: db.seekAuthor,
                value: Buffer.from(keys.id),
                indexType: "author"
              }
            }
            db.query(authorQuery, 10, (err, results) => {
              t.equal(results.length, 1)
              t.equal(results[0].id, msg1.id)

              // rerun on created index
              db.query(authorQuery, 10, (err, results) => {
                t.equal(results.length, 1)
                t.equal(results[0].id, msg1.id)

                db.query({
                  type: 'AND',
                  data: [authorQuery, typeQuery]
                }, 10, (err, results) => {
                  t.equal(results.length, 1)
                  t.equal(results[0].id, msg1.id)

                  const authorQuery2 = {
                    type: 'EQUAL',
                    data: {
                      seek: db.seekAuthor,
                      value: Buffer.from(keys2.id),
                      indexType: "author"
                    }
                  }
                  
                  db.query({
                    type: 'AND',
                    data: [typeQuery, {
                      type: 'OR',
                      data: [authorQuery, authorQuery2]
                    }]
                  }, 10, (err, results) => {
                    t.equal(results.length, 2)
                    t.end()
                  })
                })
              })
            })
          })
        })
      })
    })
  })
})
