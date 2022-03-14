<!--
SPDX-FileCopyrightText: 2021 Anders Rune Jensen

SPDX-License-Identifier: CC0-1.0
-->

# JITDB

A database on top of a [flumelog] (the recommended being
[async-append-only-log]) with automatic index generation and
maintenance.

The motivation for this database is that it should be:

- fast
- easy to understand
- run in the browser and in node

Async append only log takes care of persistance of the main log. It is
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

Before using JITDB, you have to setup an instance of
[async-append-only-log] located at a certain path. Then you can
instantiate JITDB, and it requires a **path to the directory where the
indexes** will live.

```js
const Log = require('async-append-only-log')
const JITDB = require('jitdb')

const raf = Log('/home/me/path/to/async-log', {
  blockSize: 64 * 1024,
})
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
const Log = require('async-append-only-log')
const JITDB = require('jitdb')
const {query, fromDB, where, slowEqual, toCallback} = require('jitdb/operators')

const raf = Log('/home/me/path/to/async-log', {
  blockSize: 64 * 1024,
})
const db = JITDB(raf, '/home/me/path/to/indexes')

db.onReady(() => {
  query(
    fromDB(db),
    where(slowEqual('value.content.type', 'post')),
    toCallback((err, msgs) => {
      console.log(msgs)
    })
  )
})
```

The essential operators are `fromDB`, `query`, and `toCallback`.

- **fromDB** specifies which JITDB instance we are interested in
- **query** wraps all the operators, chaining them together
- **where** wraps *descriptor operators* (see below) that narrow down the data
- **toCallback** delivers the results of the query to a callback

Then there are *descriptor operator* that help scope down the results to your
desired set of messages: `and`, `or`, `not`, `equal`, `slowEqual`, and others.

- `and(...args)` filters for messages that satisfy **all** `args`
- `or(...args)` filters for messages that satisfy **at least one** of the `args`
- `not(arg)` filters for messages that do not safisfy `arg`
- `equal(seek, value, opts)` filters for messages where a `seek`ed _field_
  matches a specific _value_:
  - `seek` is a function that takes a [bipf] buffer as input and uses
    `bipf.seekKey` to return a pointer to the _field_
  - `value` is a string or buffer which is the value we want the _field_'s value to match
  - `opts` are additional configurations:
    - `indexType` is a name used to identify the index produced by this query
    - `prefix` boolean or number `32` that tells this query to use [prefix indexes](#prefix-indexes)
- `slowEqual(objPath, value, opts)` is a more ergonomic (but slower) way of performing `equal`:
  - `objPath` a string in the shape `"foo.bar.baz"` which specifies the nested field `"baz"` inside `"bar"` inside `"foo"`
  - `value` is the same as `value` in the `equal` operator
  - `opts` same as the opts for `equal()`
- `includes(seek, value, opts)` filters for messages where a `seek`ed _field_
  is an array and includes a specific _value_
- `slowIncludes(objPath, value, opts)` is to `includes` what `slowEqual` is to
  `equal`
- `predicate(seek, fn, opts)` filters for messages where a `seek`ed _field_ is
  passed to a predicate function `fn` and the `fn` returns true
  - `opts` are additional configurations such as `indexType` and `name`. You
  SHOULD pass `opts.name` as a simple string uniquely identifying the predicate,
  OR the `fn` function should be a named function
- `slowPredicate(objPath, fn, opts)` is to `predicate` what `slowEqual` is to
  `equal`
- `absent(seek, opts)` filters for messages where a `seek`ed _field_ does not
  exist in the message
- `slowAbsent(objPath)` is to `absent` what `slowEqual` is to `equal`

Some examples:

**Get all messages of type `post`:**

```js
query(
  fromDB(db),
  where(slowEqual('value.content.type', 'post')),
  toCallback((err, msgs) => {
    console.log('There are ' + msgs.length + ' messages of type "post"')
  })
)
```

**Same as above but faster performance (recommended in production):**

```js
query(
  fromDB(db),
  where(equal(seekType, 'post', { indexType: 'type' })),
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
  where(
    and(
      slowEqual('value.content.type', 'contact'),
      or(slowEqual('value.author', aliceId), slowEqual('value.author', bobId))
    )
  ),
  toCallback((err, msgs) => {
    console.log('There are ' + msgs.length + ' messages')
  })
)
```

**Same as above but faster performance (recommended in production):**

```js
query(
  fromDB(db),
  where(
    and(
      equal(seekType, 'contact', 'type')
      or(
        equal(seekAuthor, aliceId, { indexType: 'author' }),
        equal(seekAuthor, bobId, { indexType: 'author' })
      )
    )
  ),
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
- **paginate** configures the size of each array sent to the pull-stream source
- **startFrom** configures the beginning seq from where to start streaming
- **descending** configures the pagination stream to order results
  from newest to oldest (otherwise the default order is oldest to
  newest) based on timestamp

Example, **stream all messages of type `contact` from Alice or Bob in pages of size 10:**

```js
const pull = require('pull-stream')

const source = query(
  fromDB(db),
  where(
    and(
      slowEqual('value.content.type', 'contact')
      or(slowEqual('value.author', aliceId), slowEqual('value.author', bobId)),
    ),
  ),
  paginate(10),
  toPullStream()
)

pull(
  source,
  pull.drain((msgs) => {
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
  where(
    and(
      slowEqual('value.content.type', 'contact')
      or(slowEqual('value.author', aliceId), slowEqual('value.author', bobId)),
    ),
  ),
  paginate(10),
  startFrom(15),
  descending(),
  toPullStream()
)

pull(
  source,
  pull.drain((msgs) => {
    console.log('next page:')
    console.log(msgs)
  })
)
```

**Batching** with the operator `batch()` is similar to pagination in terms of
performance, but the messages are delivered one-by-one to the final pull-stream,
instead of as any array. Example:

```js
const pull = require('pull-stream')

const source = query(
  fromDB(db),
  where(
    and(
      slowEqual('value.content.type', 'contact')
      or(slowEqual('value.author', aliceId), slowEqual('value.author', bobId)),
    ),
  ),
  batch(10), // Note `batch` instead of `paginate`
  descending(),
  toPullStream()
)

pull(
  source,
  // Note the below drain is `msg`, not `msgs` array:
  pull.drain((msg) => {
    console.log('next message:')
    console.log(msg)
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
  where(
    and(
      slowEqual('value.content.type', 'contact')
      or(slowEqual('value.author', aliceId), slowEqual('value.author', bobId)),
    ),
  ),
  toPromise()
)

console.log('There are ' + msgs.length + ' messages')
```

With pagination, **`toAsyncIter`** is like **`toPullStream`**, streaming the results in batches:

```js
const results = query(
  fromDB(db),
  where(
    and(
      slowEqual('value.content.type', 'contact')
      or(slowEqual('value.author', aliceId), slowEqual('value.author', bobId)),
    ),
  ),
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
should convert the results from those indexes to `offsets()` or
`seqs()` (read more about these in the low level API section). In
those cases, the `OFFSETS` or `SEQS` are often received
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
function task(meta, cb[, onAbort])
```

where `meta` is an object containing an instance of JITDB and other
metadata, and `onAbort` is an optional function that you can pass an
abort listener (i.e. `onAbort(() => { /* cancel my stuff */ })`).

As an example, suppose you have a custom index that returns seqs
`11`, `13` and `17`, and you want to include these results into your
operator chain, to `AND` them with a specific author. Use `deferred`
like this:

```js
query(
  fromDB(db),
  deferred((meta, cb) => {
    // do something asynchronously, then deliver results to cb
    cb(null, seqs([11, 13, 17]))
  }),
  where(slowEqual('value.author', aliceId)),
  toCallback((err, results) => {
    console.log(results)
  })
)
```

#### All operators

This is a list of all the operators supported so far:

```js
const {
  fromDB,
  query,
  where,
  and,
  or,
  not,
  equal,
  slowEqual,
  predicate,
  slowPredicate,
  absent,
  slowAbsent,
  includes,
  slowIncludes,
  gt,
  gte,
  lt,
  lte,
  deferred,
  liveSeqs,
  seqs,
  offsets,
  count,
  paginate,
  batch,
  startFrom,
  descending,
  asOffsets,
  sortByArrival,
  debug,
  toCallback,
  toPullStream,
  toPromise,
  toAsyncIter,
} = require('jitdb/operators')
```

## Prefix indexes

Most indexes in JITDB are bitvectors, which are suitable for answering
boolean queries such as "is this msg a post?" or "is this msg from
author A?". For each of these queries, JITDB creates one file.

This is fine for several cases, but some queries are not
boolean. Queries on bitvectors such as "is this msg a reply to msg X?"
can end up generating `N` files if the "msg X" can have N different
values. The creation of indexes is this case becomes the overhead.

**Prefix indexes** help in that case because they can answer
non-boolean queries with multiple different values but using just one
index file. For example, for N different values of "msg X", just one
prefix index is enough for answering "is this msg a reply to msg X?".

The way prefix indexes work is that for each message in the log, it
picks the first 32 bits of a field in the message (hence 'prefix') and
then compares your desired value with all of these prefixes. It
doesn't store the whole value because that could turn out wasteful in
storage and memory as the log scales (to 1 million or more
messages). Storing just a prefix is not enough for uniqueness, though,
as different values will have the same prefix, so queries on prefix
indexes will create false positives, but JITDB does an additional
check so in the resulting query, **you will not get false positives**.

_Rule of thumb_: use prefix indexes in an EQUAL operation only when
the target `value` of your EQUAL can dynamically assume many (more
than a dozen) possible values.

An additional option `useMap` can be provided that will store the
prefix as a map instead of an array. The map can be seen as an
inverted index that allows for faster queries at the cost of extra
space. Maps don't store empty values meaning they are also a good fit
for sparce indexes such as vote links.

It is possible specifiy where in the target the prefix buffer should
be constructed from using `prefixOffset`. This is useful for targets
that starts with a common prefix such as % in order to increase the
information amount.

## Low-level API

First some terminology: offset refers to the byte position in the log
of a message. Seq refers to the 0-based position of a message in the
log.

### paginate(operation, seq, limit, descending, onlyOffset, sortBy, cb)

Query the database returning paginated results. If one or more indexes
doesn't exist or are outdated, the indexes will be updated before the
query is run. `onlyOffset` can be used to return offset instead of the
actual messages. `sortBy` determines what timestamp to use for
ordering messages. Can take values `declared` or `arrival`. `declared`
refers to the timestamp for when a message was created, while
`arrival` refers to when a message was added to the database. This can
be important for messages from other peers that might arrive out of
order compared when they were created.

The result is an object with the fields:

- `data`: the actual messages
- `total`: the total number of messages
- `duration`: the number of ms the query took

Operation can be of the following types:

| type          | data                                         |
| ------------- | -------------------------------------------- |
| EQUAL         | { seek, value, indexType, indexAll, prefix } |
| GT,GTE,LT,LTE | { indexName, value }                         |
| OFFSETS       | { offsets }                                  |
| SEQS          | { seqs }                                     |
| NOT           | [operation]                                  |
| AND           | [operation, operation]                       |
| OR            | [operation, operation]                       |

`seek` is a function that takes a buffer from the database as input
and returns an index in the buffer from where a value can be compared
to the `value` given. `value` must be a bipf encoded value, usually
the `equal` operator will take care of that. A field not being defined
at a point in the buffer is equal to `undefined`. `prefix` enables the
use of prefix indexes for this operation. `indexType` is used to group
indexes of the same type. If `indexAll` is specified and no index of
the type and value exists, then instead of only this index being
created, missing indexes for all possible values given the seek
pointer will be created. This can be particular useful for data where
there number of different values are rather small, but still larger
than a few. One example is author or feeds in SSB, a typical database
of 1 million records will have roughly 700 authors. The biggest cost
in creating the indexes is traversing the database, so creating all
indexes in one go instead of several hundreds is a lot faster.

For `GT`, `GTE`, `LT` and `LTE`, `indexName` can be either `sequence`
or `timestamp`.

`SEQS` and `OFFSETS` allow one to use seq and offset (respectively)
positions into the log file as query operators. This is useful for
interfacing with data indexed by something else than JITDB. Seqs
are faster as they can be combined in queries directly.

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

### all(operation, seq, descending, onlyOffset, sortBy, cb)

Similar to `paginate` except there is no `limit` argument and the result
will be the messages directly.

### count(operation, seq, descending, cb)

Similar to `all` except it does not fetch records from the log, it only responds with a number that tells the total amount of records matching the operation.

### live(operation, cb)

Will setup a pull stream and this in `cb`. The pull stream will emit
new values as they are added to the underlying log. This is meant to
run after `paginate` or `all`.

Please note the index is _not_ updated when using this method and only
one live seqs stream is supported.

### reindex(offset, cb)

Reset all indexes to start from `offset`. This means that on the next
query, messages from that offset and forward will be reindexed.

### onReady(cb)

Will call when all existing indexes have been loaded.

[flumelog]: https://github.com/flumedb/
[async-append-only-log]: https://github.com/ssb-ngi-pointer/async-append-only-log
[bipf]: https://github.com/dominictarr/bipf/
[pull-stream]: https://github.com/pull-stream/pull-stream
