const bValue = new Buffer('value')
const bContent = new Buffer('content')

const bAuthor = new Buffer('author')
const bAuthorValue = new Buffer('@6CAxOI3f+LUOVrbAl0IemqiS7ATpQvr9Mdw9LC4+Uv0=.ed25519')

const bType = new Buffer('type')
const bPostValue = new Buffer('post')
const bContactValue = new Buffer('contact')

const util = require('util')
const bipf = require('bipf')
var db = require('./index')(process.argv[2], "./indexes")

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

// seems the cache needs to be warmed up to get fast results

db.query([{
  type: 'AND',
  data: [
    { type: 'EQUAL', data: { seek: seekType, value: bPostValue, indexName: "type_post" } },
    { type: 'EQUAL', data: { seek: seekAuthor, value: bAuthorValue, indexName: "author_arj" } }
  ]
}], (err, results) => {
  results.forEach(x => {
    console.log(util.inspect(x, false, null, true))
  })
})
