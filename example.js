const bValue = Buffer.from('value')
const bContent = Buffer.from('content')

const bAuthor = Buffer.from('author')
const bAuthorValue = Buffer.from('@6CAxOI3f+LUOVrbAl0IemqiS7ATpQvr9Mdw9LC4+Uv0=.ed25519')

const bType = Buffer.from('type')
const bPostValue = Buffer.from('post')
const bContactValue = Buffer.from('contact')

const util = require('util')
const bipf = require('bipf')
const FlumeLog = require('flumelog-aligned-offset')

function seekAuthor(buffer) {
  var p = 0 // note you pass in p!
  p = bipf.seekKey(buffer, p, bValue)

  if (~p)
    return bipf.seekKey(buffer, p, bAuthor)
}

function seekType(buffer) {
  var p = 0 // note you pass in p!
  p = bipf.seekKey(buffer, p, bValue)

  if (~p) {
    p = bipf.seekKey(buffer, p, bContent)
    if (~p)
      return bipf.seekKey(buffer, p, bType)
  }
}

var raf = FlumeLog(process.argv[2], {block: 64*1024})
var db = require('./index')(raf, "./indexes")
db.onReady(() => {
  // seems the cache needs to be warmed up to get fast results

  console.time("get all posts from user")

  db.query({
    type: 'AND',
    data: [
      { type: 'EQUAL', data: { seek: seekType, value: bPostValue, indexName: "type_post" } },
      { type: 'EQUAL', data: { seek: seekAuthor, value: bAuthorValue, indexName: "author_arj" } }
    ]
  }, false, (err, results) => {
    console.timeEnd("get all posts from user")
    console.log(results.length)
  })

  /*
    db.query({
    type: 'AND',
    data: [
    { type: 'EQUAL', data: { seek: seekType, value: bPostValue, indexName: "type_post" } },
    { type: 'EQUAL', data: { seek: seekAuthor, value: bAuthorValue, indexName: "author_arj" } }
    ]
    }, true, (err, results) => {
    results.forEach(x => {
    console.log(util.inspect(x, false, null, true))
    })
    })
  */

  /*
    db.query({
    type: 'AND',
    data: [
    { type: 'EQUAL', data: { seek: seekAuthor, value: bAuthorValue, indexName: "author_arj" } },
    {
    type: 'OR',
    data: [
    { type: 'EQUAL', data: { seek: seekType, value: bPostValue, indexName: "type_post" } },
    { type: 'EQUAL', data: { seek: seekType, value: bContactValue, indexName: "type_contact" } },
    ]
    }
    ]
    }, true, (err, results) => {
    results.forEach(x => {
    console.log(util.inspect(x, false, null, true))
    })
    })
  */
})
