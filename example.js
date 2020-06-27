const bValue = new Buffer('value')
const bAuthor = new Buffer('author')

const authorValue = '@6CAxOI3f+LUOVrbAl0IemqiS7ATpQvr9Mdw9LC4+Uv0=.ed25519'
const bAuthorValue = new Buffer(authorValue)

const util = require('util')
const bipf = require('bipf')
var db = require('./index')(process.argv[2], "./indexes")

function seekAuthor(buffer) {
  var p = 0 // note you pass in p!
  p = bipf.seekKey(buffer, p, bValue)

  return bipf.seekKey(buffer, p, bAuthor)
}

db.query(seekAuthor, bAuthorValue, "author_arj", (err, results) => {
  results.forEach(x => {
    console.log(util.inspect(x, false, null, true))
  })
})
