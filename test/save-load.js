const test = require('tape')
const path = require('path')
const TypedFastBitSet = require('typedfastbitset')
const { prepareAndRunTest, addMsg } = require('./common')()

const dir = '/tmp/jitdb-save-load'
require('rimraf').sync(dir)

prepareAndRunTest('SaveLoad', dir, (t, db, raf) => {
  var data = new TypedFastBitSet()
  for (var i = 0; i < 10; i += 2)
    data.add(i)

  db.saveIndex("test", 1, data, (err) => {
    const file = path.join(dir, "indexesSaveLoad", "test.index")
    db.loadIndex(file, Uint32Array, (err, index) => {
      let loadedData = new TypedFastBitSet(index.data)
      loadedData.words = index.data
      loadedData.count = index.count
      t.deepEqual(data.array(), loadedData.array())

      loadedData.add(10)

      db.saveIndex("test", 1, loadedData, (err) => {
        db.loadIndex(file, Uint32Array, (err, index) => {
          let loadedData2 = new TypedFastBitSet(index.data)
          loadedData2.words = index.data
          loadedData2.count = index.count
          t.deepEqual(loadedData.array(), loadedData2.array())
          t.end()
        })
      })
    })
  })
})
