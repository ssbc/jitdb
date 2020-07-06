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

[flumelog-aligned-offset]: https://github.com/flumedb/flumelog-aligned-offset
[bipf]: https://github.com/dominictarr/bipf/

