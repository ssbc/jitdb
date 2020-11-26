const test = require('tape')
const path = require('path')
const TypedFastBitSet = require('typedfastbitset')
const { prepareAndRunTest, addMsg } = require('./common')()
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')

const dir = '/tmp/jitdb-save-load'
rimraf.sync(dir)
mkdirp.sync(dir)

prepareAndRunTest('SaveLoad', dir, (t, db, raf) => {
  var bitset = new TypedFastBitSet()
  for (var i = 0; i < 10; i += 2) bitset.add(i)

  db.saveIndex('test', 123, bitset, (err) => {
    const file = path.join(dir, 'indexesSaveLoad', 'test.index')
    db.loadIndex(file, Uint32Array, (err, index) => {
      let loadedBitset = new TypedFastBitSet()
      loadedBitset.words = index.tarr
      loadedBitset.count = index.count
      t.deepEqual(bitset.array(), loadedBitset.array())

      loadedBitset.add(10)

      db.saveIndex('test', 1234, loadedBitset, (err) => {
        db.loadIndex(file, Uint32Array, (err, index) => {
          let loadedBitset2 = new TypedFastBitSet()
          loadedBitset2.words = index.tarr
          loadedBitset2.count = index.count
          t.deepEqual(loadedBitset.array(), loadedBitset2.array())
          t.end()
        })
      })
    })
  })
})

prepareAndRunTest('SaveLoadOffset', dir, (t, db, raf) => {
  var tarr = new Uint32Array(16 * 1000)
  for (var i = 0; i < 10; i += 1) tarr[i] = i

  db.saveTypedArray('test', 123, 10, tarr, (err) => {
    const file = path.join(dir, 'indexesSaveLoadOffset', 'test.index')
    db.loadIndex(file, Uint32Array, (err, loadedIdx) => {
      for (var i = 0; i < 10; i += 1) t.equal(tarr[i], loadedIdx.tarr[i])

      loadedIdx.tarr[10] = 10

      db.saveTypedArray('test', 1234, 11, loadedIdx.tarr, (err) => {
        db.loadIndex(file, Uint32Array, (err, loadedIdx2) => {
          for (var i = 0; i < 11; i += 1)
            t.equal(loadedIdx.tarr[i], loadedIdx2.tarr[i])
          t.end()
        })
      })
    })
  })
})

prepareAndRunTest('SaveLoadTimestamp', dir, (t, db, raf) => {
  var tarr = new Float64Array(16 * 1000)
  for (var i = 0; i < 10; i += 1) tarr[i] = i * 1000000

  db.saveTypedArray('test', 123, 10, tarr, (err) => {
    const file = path.join(dir, 'indexesSaveLoadTimestamp', 'test.index')
    db.loadIndex(file, Float64Array, (err, loadedIdx) => {
      for (var i = 0; i < 10; i += 1) t.equal(tarr[i], loadedIdx.tarr[i])

      loadedIdx.tarr[10] = 10 * 1000000

      db.saveTypedArray('test', 1234, 11, loadedIdx.tarr, (err) => {
        db.loadIndex(file, Float64Array, (err, loadedIdx2) => {
          for (var i = 0; i < 11; i += 1)
            t.equal(loadedIdx.tarr[i], loadedIdx2.tarr[i])
          t.end()
        })
      })
    })
  })
})
