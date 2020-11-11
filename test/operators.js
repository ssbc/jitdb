const test = require('tape');
const pull = require('pull-stream');
const validate = require('ssb-validate');
const ssbKeys = require('ssb-keys');
const {prepareAndRunTest, addMsg} = require('./common')();
const rimraf = require('rimraf');
const mkdirp = require('mkdirp');
const {
  query,
  and,
  or,
  type,
  author,
  fromDB,
  paginate,
  startFrom,
  ascending,
  toCallback,
  toPromise,
  toPullStream,
  toAsyncIter,
} = require('../operators');

const dir = '/tmp/jitdb-query-api';
rimraf.sync(dir);
mkdirp.sync(dir);

const alice = ssbKeys.generate('ed25519', Buffer.alloc(32, 'a'));
const bob = ssbKeys.generate('ed25519', Buffer.alloc(32, 'b'));

prepareAndRunTest('operators API returns objects', dir, (t, db, raf) => {
  const queryTree = query(fromDB(db), and(type('post')));

  t.equal(typeof queryTree, 'object', 'queryTree is an object');

  t.equal(queryTree.type, 'EQUAL');

  t.equal(queryTree.data.indexType, 'type');
  t.deepEqual(queryTree.data.value, Buffer.from('post'));
  t.true(queryTree.data.seek.toString().includes('bipf.seekKey'));

  t.equal(typeof queryTree.meta, 'object', 'queryTree contains meta');
  t.equal(typeof queryTree.meta.db, 'object', 'queryTree contains meta.db');
  t.equal(
    typeof queryTree.meta.db.onReady,
    'function',
    'meta.db looks correct',
  );

  t.end();
});

prepareAndRunTest('operators API supports and or', dir, (t, db, raf) => {
  const queryTree = query(
    fromDB(db),
    and(type('post')),
    and(or(author(alice.id), author(bob.id))),
  );

  t.equal(typeof queryTree, 'object', 'queryTree is an object');

  t.equal(queryTree.type, 'AND');
  t.true(Array.isArray(queryTree.data), '.data is an array');

  t.equal(queryTree.data[0].type, 'EQUAL');
  t.equal(queryTree.data[0].data.indexType, 'type');
  t.deepEqual(queryTree.data[0].data.value, Buffer.from('post'));

  t.equal(queryTree.data[1].type, 'OR');
  t.true(Array.isArray(queryTree.data[1].data), '.data[1].data is an array');

  t.equal(queryTree.data[1].data[0].type, 'EQUAL');
  t.deepEqual(queryTree.data[1].data[0].data.indexType, 'author');
  t.deepEqual(queryTree.data[1].data[0].data.value, Buffer.from(alice.id));
  t.true(
    queryTree.data[1].data[0].data.seek.toString().includes('bipf.seekKey'),
  );

  t.equal(queryTree.data[1].data[1].type, 'EQUAL');
  t.equal(queryTree.data[1].data[1].data.indexType, 'author');
  t.deepEqual(queryTree.data[1].data[1].data.value, Buffer.from(bob.id));
  t.true(
    queryTree.data[1].data[1].data.seek.toString().includes('bipf.seekKey'),
  );

  t.end();
});

prepareAndRunTest('operators multi and', dir, (t, db, raf) => {
  const queryTree = query(
    fromDB(db),
    and(type('post'), author(alice.id), author(bob.id)),
  );

  t.equal(typeof queryTree, 'object', 'queryTree is an object');

  t.equal(queryTree.type, 'AND');
  t.true(Array.isArray(queryTree.data), '.data is an array');

  t.equal(queryTree.data[0].type, 'EQUAL');
  t.equal(queryTree.data[1].type, 'EQUAL');
  t.equal(queryTree.data[2].type, 'EQUAL');

  t.end();
});

prepareAndRunTest('operators multi or', dir, (t, db, raf) => {
  const queryTree = query(
    fromDB(db),
    or(type('post'), author(alice.id), author(bob.id)),
  );

  t.equal(typeof queryTree, 'object', 'queryTree is an object');

  t.equal(queryTree.type, 'OR');
  t.true(Array.isArray(queryTree.data), '.data is an array');

  t.equal(queryTree.data[0].type, 'EQUAL');
  t.equal(queryTree.data[1].type, 'EQUAL');
  t.equal(queryTree.data[2].type, 'EQUAL');

  t.end();
});

prepareAndRunTest(
  'operators paginate startFrom ascending',
  dir,
  (t, db, raf) => {
    const queryTreePaginate = query(
      fromDB(db),
      and(type('post')),
      paginate(10),
    );

    const queryTreeStartFrom = query(
      fromDB(db),
      and(type('post')),
      startFrom(5),
    );

    const queryTreeAscending = query(
      fromDB(db),
      and(type('post')),
      ascending(),
    );

    const queryTreeAll = query(
      fromDB(db),
      and(type('post')),
      startFrom(5),
      paginate(10),
      ascending(),
    );

    t.equal(queryTreePaginate.meta.pageSize, 10);
    t.equal(queryTreeStartFrom.meta.offset, 5);
    t.equal(queryTreeAscending.meta.reverse, true);

    t.equal(queryTreeAll.meta.pageSize, 10);
    t.equal(queryTreeAll.meta.offset, 5);
    t.equal(queryTreeAll.meta.reverse, true);

    t.end();
  },
);

// FIXME: build support for this, then uncomment it:
// prepareAndRunTest('operators fromDB then toCallback', dir, (t, db, raf) => {
//   const msg = {type: 'post', text: 'Testing!'};
//   let state = validate.initial();
//   state = validate.appendNew(state, null, alice, msg, Date.now());
//   state = validate.appendNew(state, null, bob, msg, Date.now());

//   addMsg(state.queue[0].value, raf, (e1, msg1) => {
//     addMsg(state.queue[1].value, raf, (e2, msg2) => {
//       query(
//         fromDB(db),
//         toCallback((err, msgs) => {
//           t.error(err, 'toCallback got no error');
//           t.equal(msgs.length, 2, 'toCallback got two messages');
//           t.equal(msgs[0].value.author, alice.id);
//           t.equal(msgs[0].value.content.type, 'post');
//           t.equal(msgs[1].value.author, bob.id);
//           t.equal(msgs[1].value.content.type, 'post');
//           t.end();
//         }),
//       );
//     });
//   });
// });

prepareAndRunTest('operators toCallback', dir, (t, db, raf) => {
  const msg = {type: 'post', text: 'Testing!'};
  let state = validate.initial();
  state = validate.appendNew(state, null, alice, msg, Date.now());
  state = validate.appendNew(state, null, bob, msg, Date.now());

  addMsg(state.queue[0].value, raf, (e1, msg1) => {
    addMsg(state.queue[1].value, raf, (e2, msg2) => {
      query(
        fromDB(db),
        or(author(alice.id), author(bob.id)),
        toCallback((err, msgs) => {
          t.error(err, 'toCallback got no error');
          t.equal(msgs.length, 2, 'toCallback got two messages');
          t.equal(msgs[0].value.author, alice.id);
          t.equal(msgs[0].value.content.type, 'post');
          t.equal(msgs[1].value.author, bob.id);
          t.equal(msgs[1].value.content.type, 'post');
          t.end();
        }),
      );
    });
  });
});

prepareAndRunTest('operators toPromise', dir, (t, db, raf) => {
  const msg = {type: 'post', text: 'Testing!'};
  let state = validate.initial();
  state = validate.appendNew(state, null, alice, msg, Date.now());
  state = validate.appendNew(state, null, bob, msg, Date.now());

  addMsg(state.queue[0].value, raf, (e1, msg1) => {
    addMsg(state.queue[1].value, raf, (e2, msg2) => {
      query(
        fromDB(db),
        or(author(alice.id), author(bob.id)),
        toPromise(),
      ).then(
        (msgs) => {
          t.equal(msgs.length, 2, 'toPromise got two messages');
          t.equal(msgs[0].value.author, alice.id);
          t.equal(msgs[0].value.content.type, 'post');
          t.equal(msgs[1].value.author, bob.id);
          t.equal(msgs[1].value.content.type, 'post');
          t.end();
        },
        (err) => {
          t.fail(err);
        },
      );
    });
  });
});

prepareAndRunTest('operators toPullStream', dir, (t, db, raf) => {
  const msg = {type: 'post', text: 'Testing!'};
  let state = validate.initial();
  state = validate.appendNew(state, null, alice, msg, Date.now());
  state = validate.appendNew(state, null, bob, msg, Date.now());

  addMsg(state.queue[0].value, raf, (e1, msg1) => {
    addMsg(state.queue[1].value, raf, (e2, msg2) => {
      pull(
        query(
          fromDB(db),
          or(author(alice.id), author(bob.id)),
          paginate(2),
          ascending(),
          toPullStream(),
        ),
        pull.collect((err, pages) => {
          t.error(err, 'toPullStream got no error');
          t.equal(pages.length, 1, 'toPullStream got one page');
          const msgs = pages[0]
          t.equal(msgs.length, 2, 'page has two messages');
          t.equal(msgs[0].value.author, alice.id);
          t.equal(msgs[0].value.content.type, 'post');
          t.equal(msgs[1].value.author, bob.id);
          t.equal(msgs[1].value.content.type, 'post');
          t.end();
        }),
      );
    });
  });
});

prepareAndRunTest('operators toAsyncIter', dir, (t, db, raf) => {
  const msg = {type: 'post', text: 'Testing!'};
  let state = validate.initial();
  state = validate.appendNew(state, null, alice, msg, Date.now());
  state = validate.appendNew(state, null, bob, msg, Date.now());

  addMsg(state.queue[0].value, raf, (e1, msg1) => {
    addMsg(state.queue[1].value, raf, async (e2, msg2) => {
      try {
        let i = 0
        const results = query(
          fromDB(db),
          or(author(alice.id), author(bob.id)),
          paginate(2),
          ascending(),
          toAsyncIter(),
        )
        for await (let page of results) {
          t.equal(i, 0, 'just one page')
          i += 1;
          const msgs = page
          t.equal(msgs.length, 2, 'page has two messages');
          t.equal(msgs[0].value.author, alice.id);
          t.equal(msgs[0].value.content.type, 'post');
          t.equal(msgs[1].value.author, bob.id);
          t.equal(msgs[1].value.content.type, 'post');
          t.end();
        }
      } catch (err) {
        t.fail(err)
      }
    });
  });
});
