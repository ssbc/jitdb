# JITDB

A database on top of a [flumelog] (the recommended being
[async-flumelog]) with automatic index generation and maintenance.

The motivation for this database is that it should be:
 - fast
 - easy to understand
 - run in the browser and in node

Async flumelog takes care of persistance of the main log. It is
expected to use [bipf] to encode data. On top of this, JITDB lazily
creates and maintains indexes based on the way the data is queried.
Meaning if you search for messages of type `post` an author `x` two
indexes will be created the first time. One for type and one for
author. Specific indexes will only updated when it is queried again.
These indexes are tiny compared to normal flume indexes. An index of
type `post` is 80kb.

For this to be feasible it must be really fast to do a full log scan.
It turns out that the combination of push streams and bipf makes
streaming the full log not much slower than reading the file. Meaning
a 350mb log can be scanned in a few seconds.

While this is mainly aimed as a query engine, it is possible to base
other indexes types on top of this, such as a reduce index on contact
messages.

## API

### Setup

Before using JITDB, you have to setup an instance of [async-flumelog]
located at a certain path. Then you can instantiate JITDB, and it
requires a **path to the directory where the indexes** will live.

```js
const FlumeLog = require('async-flumelog')
const JITDB = require('jitdb')

const raf = FlumeLog('/home/me/path/to/async-flumelog', {blockSize: 64*1024})
const db = JITDB(raf, '/home/me/path/to/indexes')

db.onReady(() => {
  // The db is ready to be queried
})
```

### Operators

JITDB comes with a set of composable "operators" that allow you to
query the database. You can load these operators from
`require('jitdb/operators')`.

```js
const FlumeLog = require('async-flumelog')
const JITDB = require('jitdb')
const {query, fromDB, and, slowEqual, toCallback} = require('jitdb/operators')

const raf = FlumeLog('/home/me/path/to/async-flumelog', {blockSize: 64*1024})
const db = JITDB(raf, '/home/me/path/to/indexes')

db.onReady(() => {
  query(
    fromDB(db),
    and(slowEqual('value.content.type', 'post')),
    toCallback((err, msgs) => {
      console.log(msgs)
    })
  )
})
```

The essential operators are `fromDB`, `query`, and `toCallback`.

- **fromDB** specifies which JITDB instance we are interested in
- **query** wraps all the operators, chaining them together
- **toCallback** delivers the results of the query to a callback

Then there are filtering operators that scope down the results to your
desired set of messages: `and`, `or`, `equal`, `slowEqual`.

- **and** filters for messages that satisfy **all** of the arguments provided
- **or** filters for messages that satisfy **at least one** of the arguments provided
- **equal** filters for messages that have a specific *field*, arguments are:
  - `seek` is a function that takes a [bipf] buffer as input and uses
    `bipf.seekKey` to return a pointer to the *field*
  - `value` is a string or buffer which is the value we want the *field*'s value to match
  - `indexType` is a name used to identify the index produced by this query
- **slowEqual** is a more ergonomic (but slower) way of performing `equal`, the arguments are:
  - `seekDescriptor` a string in the shape `"foo.bar.baz"` which specifies the nested field `"baz"`
  - `value` is the same as `value` in the `equal` operator

Some examples:

**Get all messages of type `post`:**

```js
query(
  fromDB(db),
  and(slowEqual('value.content.type', 'post')),
  toCallback((err, msgs) => {
    console.log('There are ' + msgs.length + ' messages of type "post"')
  })
)
```

**Same as above but faster performance (recommended in production):**

```js
query(
  fromDB(db),
  and(equal(seekType, 'post', 'type')),
  toCallback((err, msgs) => {
    console.log('There are ' + msgs.length + ' messages of type "post"')
  })
)

// The `seekType` function takes a buffer and uses `bipf` APIs to search for
// the fields we want.
const bValue = Buffer.from('value') // better for performance if defined outside
const bContent = Buffer.from('content')
const bType = Buffer.from('type')
function seekType(buffer) {
  var p = 0 // p stands for "position" in the buffer, offset from start
  p = bipf.seekKey(buffer, p, bValue)
  if (p < 0) return
  p = bipf.seekKey(buffer, p, bContent)
  if (p < 0) return
  return bipf.seekKey(buffer, p, bType)
}
```

**Get all messages of type `contact` from Alice or Bob:**

```js
query(
  fromDB(db),
  and(slowEqual('value.content.type', 'contact')),
  and(or(slowEqual('value.author', aliceId), slowEqual('value.author', bobId))),
  toCallback((err, msgs) => {
    console.log('There are ' + msgs.length + ' messages')
  })
)
```

**Same as above but faster performance (recommended in production):**

```js
query(
  fromDB(db),
  and(equal(seekType, 'contact', 'type')),
  and(or(equal(seekAuthor, aliceId, 'author'), equal(seekAuthor, bobId, 'author'))),
  toCallback((err, msgs) => {
    console.log('There are ' + msgs.length + ' messages')
  })
)

// where seekAuthor is
const bValue = Buffer.from('value') // better for performance if defined outside
const bAuthor = Buffer.from('author')
function seekAuthor(buffer) {
  var p = 0
  p = bipf.seekKey(buffer, p, bValue)
  if (p < 0) return
  return bipf.seekKey(buffer, p, bAuthor)
}
```

#### Pagination

If you use `toCallback`, it gives you all results in one go. If you
want to get results in batches, you should use **`toPullStream`**,
**`paginate`**, and optionally `startFrom` and `descending`.

- **toPullStream** creates a [pull-stream] source to stream the results
- **paginate** configures the size of each page stream to the pull-stream source
- **startFrom** configures the beginning offset from where to start streaming
- **descending** configures the pagination stream to order results
  from newest to oldest (otherwise the default order is oldest to
  newest)

Example, **stream all messages of type `contact` from Alice or Bob in pages of size 10:**

```js
const pull = require('pull-stream')

const source = query(
  fromDB(db),
  and(slowEqual('value.content.type', 'contact')),
  and(or(slowEqual('value.author', aliceId), slowEqual('value.author', bobId))),
  paginate(10),
  toPullStream()
)

pull(
  source,
  pull.drain(msgs => {
    console.log('next page')
    console.log(msgs)
  })
)
```

**Stream all messages of type `contact` from Alice or Bob in pages of
size 10, starting from the 15th message, sorted from newest to
oldest:**

```js
const pull = require('pull-stream')

const source = query(
  fromDB(db),
  and(slowEqual('value.content.type', 'contact')),
  and(or(slowEqual('value.author', aliceId), slowEqual('value.author', bobId))),
  paginate(10),
  startFrom(15),
  descending(),
  toPullStream()
)

pull(
  source,
  pull.drain(msgs => {
    console.log('next page:')
    console.log(msgs)
  })
)
```

#### async/await

There are also operators that support getting the values using
`await`. **`toPromise`** is like `toCallback`, delivering all results
at once:

```js
const msgs = await query(
  fromDB(db),
  and(slowEqual('value.content.type', 'contact')),
  and(or(slowEqual('value.author', aliceId), slowEqual('value.author', bobId))),
  toPromise()
)

console.log('There are ' + msgs.length + ' messages')
```

With pagination, **`toAsyncIter`** is like **`toPullStream`**, streaming the results in batches:

```js
const results = query(
  fromDB(db),
  and(slowEqual('value.content.type', 'contact')),
  and(or(slowEqual('value.author', aliceId), slowEqual('value.author', bobId))),
  paginate(10),
  startFrom(15),
  toAsyncIter()
)

for await (let msgs of results) {
  console.log('next page:')
  console.log(msgs)
}
```

#### Custom indexes and `deferred` operator

There may be custom indexes external to JITDB, in which case you
should convert the results from those indexes to `seqs()` or
`offsets()` (read more about these in the low level API section). In
those cases, the `SEQS` or `OFFSETS` are often received
asynchronously. To support piping these async results in the `query`
chain, we have the `deferred()` operator which postpones the fetching
of results from your custom index, but allows you to compose
operations nevertheless.

```js
// operator
deferred(task)
```

where `task` is any function of the format

```js
function task(meta, cb)
```

where `meta` is an object containing an instance of JITDB and other
metadata.

As an example, suppose you have a custom index that returns offsets
`11`, `13` and `17`, and you want to include these results into your
operator chain, to `AND` them with a specific author. Use `deferred`
like this:

```js
query(
  fromDB(db),
  deferred((meta, cb) => {
    // do something asynchronously, then deliver results to cb
    cb(null, offsets([11, 13, 17]))
  }),
  and(slowEqual('value.author', aliceId)),
  toCallback((err, results) => {
    console.log(results)
  }),
)
```

#### All operators

This is a list of all the operators supported so far:

```js
const {
  fromDB,
  query,
  and,
  or,
  equal,
  slowEqual,
  gt,
  gte,
  lt,
  lte,
  deferred,
  seqs,
  offsets,
  paginate,
  startFrom,
  descending,
  debug,
  toCallback,
  toPullStream,
  toPromise,
  toAsyncIter
} = require('jitdb/operators')
```

## Low-level API

### paginate(operation, offset, limit, descending, cb)

Query the database returning paginated results. If one or more indexes
doesn't exist or are outdated, the indexes will be updated before the
query is run. The result is an object with the fields:

 - `data`: the actual messages
 - `total`: the total number of messages
 - `duration`: the number of ms the query took

Operation can be of the following types:

| type          | data |
| ------------- | ---- |
| EQUAL         | { seek, value, indexType, indexAll } |
| GT,GTE,LT,LTE | { indexName, value } |
| SEQS          | { seqs } |
| OFFSETS       | { offsets } |
| AND           | [operation, operation] |
| OR            | [operation, operation] |

`seek` is a function that takes a buffer from the database as input
and returns an index in the buffer from where a value can be compared
to the `value` given. If value is `undefined` it corresponds to the
field not being defined at that point in the buffer. `indexType` is
used to group indexes of the same type. If `indexAll` is specified and
no index of the type and value exists, then instead of only this index
being created, missing indexes for all possible values given the seek
pointer will be created. This can be particular useful for data where
there number of different values are rather small, but still larger
than a few. One example is author or feeds in SSB, a typical database
of 1 million records will have roughly 700 authors. The biggest cost
in creating the indexes is traversing the database, so creating all
indexes in one go instead of several hundreds is a lot faster.

For `GT`, `GTE`, `LT` and `LTE`, `indexName` can be either `sequence`
or `timestamp`.

`OFFSETS` and `SEQS` allow one to use offset and seq (respectively) positions
into the log file as query operators. This is useful for interfacing with data indexed by something else than JITDB. Offsets are faster as they can be combined
in queries directly.

Example

```
{
  type: 'AND',
  data: [
    { type: 'EQUAL', data: { seek: db.seekType, value: 'post', indexType: "type" } },
    { type: 'EQUAL', data: { seek: db.seekAuthor, value: '@6CAxOI3f+LUOVrbAl0IemqiS7ATpQvr9Mdw9LC4+Uv0=.ed25519', indexType: "author" } }
  ]
}
```

I considered adding an option to return raw buffers in order to do
some after processing that you wouldn't create and index for, but the
overhead of decoding the buffers is small enough that I don't think it
makes sense.

### all(operation, offset, descending, cb)

Similar to paginate except there is no `limit` argument and the result
will be the messages directly.

### querySeq(operation, seq, cb)

Executes the `operation` but takes a database seq and returns all
results added after the seq. This can be useful to keep an external
data structure in sync with the result of a query.

### getSeq(operation)

Get the latest seq of an index

### live(operation, cb)

Will setup a pull stream and this in `cb`. The pull stream will emit
new values as they are added to the underlying log. This is meant to
run after `paginate` or `all`.

Please note the index is *not* updated when using this method and only
one live offsets stream is supported.

### onReady(cb)

Will call when all existing indexes have been loaded.

[flumelog]: https://github.com/flumedb/
[async-flumelog]: https://github.com/flumedb/async-flumelog
[bipf]: https://github.com/dominictarr/bipf/
[pull-stream]: https://github.com/pull-stream/pull-stream
