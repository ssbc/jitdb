# JITDB

A database on top of a [flumelog-aligned-offset] with automatic index
generation and maintenance.

Current status: Work In Progress

The motivation for this database is that it should be:
 - fast
 - easy to understand
 - run in the browser and in node

Flumelog-aligned-offset takes care of persistance of the main log. It
is expected to use [bipf] to encode data. On top of this, JITDB lazily
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

### query(operation, limit, cb)

Query the database. If one or more indexes doesn't exist or are
outdated, the indexes will be updated before the query is run. If
`limit` is specified, the top results based on timestamp will be
returned in cb.

Operation can be of the following types:

| type  | data |
| ----- | ---- |
| EQUAL | { seek, value, indexType } |
| AND   | [operation, operation] |
| OR    | [operation, operation] |

Example

```
{
  type: 'AND',
  data: [
    { type: 'EQUAL', data: { seek: db.seekType, value: Buffer.from('post'), indexType: "type" } },
    { type: 'EQUAL', data: { seek: db.seekAuthor, value: Buffer.from('@6CAxOI3f+LUOVrbAl0IemqiS7ATpQvr9Mdw9LC4+Uv0=.ed25519'), indexType: "author" } }
  ]
}
```

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


[flumelog-aligned-offset]: https://github.com/flumedb/flumelog-aligned-offset
[bipf]: https://github.com/dominictarr/bipf/

