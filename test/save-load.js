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

  db.saveIndex("test", 123, data, (err) => {
    const file = path.join(dir, "indexesSaveLoad", "test.index")
    db.loadIndex(file, Uint32Array, (err, index) => {
      let loadedData = new TypedFastBitSet()
      loadedData.words = index.data
      loadedData.count = index.count
      t.deepEqual(data.array(), loadedData.array())

      loadedData.add(10)

      db.saveIndex("test", 1234, loadedData, (err) => {
        db.loadIndex(file, Uint32Array, (err, index) => {
          let loadedData2 = new TypedFastBitSet()
          loadedData2.words = index.data
          loadedData2.count = index.count
          t.deepEqual(loadedData.array(), loadedData2.array())
          t.end()
        })
      })
    })
  })
})

prepareAndRunTest('SaveLoadOffset', dir, (t, db, raf) => {
  var data = new Uint32Array(16 * 1000)
  for (var i = 0; i < 10; i += 1)
    data[i] = i

  db.saveTypedArray("test", 123, 10, data, (err) => {
    const file = path.join(dir, "indexesSaveLoadOffset", "test.index")
    db.loadIndex(file, Uint32Array, (err, loadedData) => {
      for (var i = 0; i < 10; i += 1)
        t.equal(data[i], loadedData.data[i])

      loadedData.data[10] = 10

      db.saveTypedArray("test", 1234, 11, loadedData.data, (err) => {
        db.loadIndex(file, Uint32Array, (err, loadedData2) => {
          for (var i = 0; i < 11; i += 1)
            t.equal(loadedData.data[i], loadedData2.data[i])
          t.end()
        })
      })
    })
  })
})

prepareAndRunTest('SaveLoadTimestamp', dir, (t, db, raf) => {
  var data = new Float64Array(16 * 1000)
  for (var i = 0; i < 10; i += 1)
    data[i] = i * 1000000

  db.saveTypedArray("test", 123, 10, data, (err) => {
    const file = path.join(dir, "indexesSaveLoadTimestamp", "test.index")
    db.loadIndex(file, Float64Array, (err, loadedData) => {
      for (var i = 0; i < 10; i += 1)
        t.equal(data[i], loadedData.data[i])

      loadedData.data[10] = 10 * 1000000

      db.saveTypedArray("test", 1234, 11, loadedData.data, (err) => {
        db.loadIndex(file, Float64Array, (err, loadedData2) => {
          for (var i = 0; i < 11; i += 1)
            t.equal(loadedData.data[i], loadedData2.data[i])
          t.end()
        })
      })
    })
  })
})
