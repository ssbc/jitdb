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

    const typeQuery = {
      type: 'EQUAL',
      data: {
        seek: db.seekType,
        value: Buffer.from('post'),
        indexType: "type"
      }
    }

    var i = 1
    db.liveQuerySingleIndex(typeQuery, (err, results) => {
      if (i++ == 1)
      {
        t.equal(results.length, 1)
        t.equal(results[0].id, state.queue[0].value.id)
        addMsg(state.queue[1].value, (err, msg1) => {})
      }
      else {
        t.equal(results.length, 1)
        t.equal(results[0].id, state.queue[1].value.id)
        t.end()
      }
    })
    
    addMsg(state.queue[0].value, (err, msg1) => {
      console.log("waiting for live query")
    })
  })
})
