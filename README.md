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

### query(operation, offset, limit, reverse, cb)

Query the database. If one or more indexes doesn't exist or are
outdated, the indexes will be updated before the query is run. If
`limit` and `offset` is specified, they will be used to return of view
of the top results based on timestamp. `reverse` can be used together
with `limit` and `offset` and is also optional.

Operation can be of the following types:

| type          | data |
| ------------- | ---- |
| EQUAL         | { seek, value, indexType, indexAll } |
| GT,GTE,LT,LTE | { indexName, value } |
| DATA          | { seqs, offsets } |
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

For `GT`, 'GTE', 'LT' and 'LTE', `indexName` can be either `sequence`
or `timestamp`.

`DATA` allows one to use offset or seq positions into the log file as
query operators. This is useful for interfacing with data indexed by
something else than JITDB. Offsets are faster as they can be combined
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

### querySeq(operation, seq, cb)

Behave similar to query except it takes a database seq and returns all
results added after the seq. This can be useful to keep an external
data structure in sync with the result of a query.

### getSeq(operation)

Get the latest seq of an index

### liveQuerySingleIndex(operation, cb)

Operation must be a single index, such as the contact index. With this
results matching the index will be returned in callback as they are
added to the database. The index is *not* updated when using this method.

### onReady(cb)

Will call when all existing indexes have been loaded.

### seekAuthor(buffer)

A helper seek function for queries

### seekType(buffer)

A helper seek function for queries

### seekRoot(buffer)

A helper seek function for queries

### seekPrivate(buffer)

A helper seek function for queries


[flumelog]: https://github.com/flumedb/
[async-flumelog]: https://github.com/flumedb/async-flumelog
[bipf]: https://github.com/dominictarr/bipf/

