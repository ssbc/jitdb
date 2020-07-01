const util = require('util')
const bipf = require('bipf')
const FlumeLog = require('flumelog-aligned-offset')

const bAuthorValue = Buffer.from('@6CAxOI3f+LUOVrbAl0IemqiS7ATpQvr9Mdw9LC4+Uv0=.ed25519')
const bPostValue = Buffer.from('post')
const bContactValue = Buffer.from('contact')

var raf = FlumeLog(process.argv[2], {block: 64*1024})
var db = require('./index')(raf, "./indexes")
db.onReady(() => {
  // seems the cache needs to be warmed up to get fast results

  db.query({
    type: 'AND',
    data: [
      { type: 'EQUAL', data: { seek: db.seekType, value: bPostValue, indexType: "type" } },
      { type: 'EQUAL', data: { seek: db.seekRoot, value: undefined, indexType: "root" } }
      ]
  }, 10, (err, results) => {
    results.forEach(x => {
      console.log(util.inspect(x, false, null, true))
    })
  })

  return
  
  console.time("get all posts from user")

  db.query({
    type: 'AND',
    data: [
      { type: 'EQUAL', data: { seek: db.seekType, value: bPostValue, indexType: "type" } },
      { type: 'EQUAL', data: { seek: db.seekAuthor, value: bAuthorValue, indexType: "author" } }
    ]
  }, 0, (err, results) => {
    console.timeEnd("get all posts from user")

    console.time("get last 10 posts from user")

    db.query({
      type: 'AND',
      data: [
        { type: 'EQUAL', data: { seek: db.seekType, value: bPostValue, indexType: "type" } },
        { type: 'EQUAL', data: { seek: db.seekAuthor, value: bAuthorValue, indexType: "author" } }
      ]
    }, 10, (err, results) => {
      console.timeEnd("get last 10 posts from user")

      var hops = {}
      const query = { type: 'EQUAL', data: { seek: db.seekType, value: bContactValue, indexType: "type" } }
      const isFeed = require('ssb-ref').isFeed

      console.time("contacts")

      db.query(query, 0, (err, results) => {
        results.forEach(data => {
          var from = data.value.author
          var to = data.value.content.contact
          var value =
              data.value.content.blocking || data.value.content.flagged ? -1 :
              data.value.content.following === true ? 1
              : -2

          if(isFeed(from) && isFeed(to)) {
            hops[from] = hops[from] || {}
            hops[from][to] = value
          }
        })

        console.timeEnd("contacts")
        //console.log(hops)
      })
    })
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
