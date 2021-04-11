const tape = require('tape');

const skipCreate = process.argv[2] === 'noCreate' || !!process.env.GET_BENCHMARK_MATRIX || !!process.env.CURRENT_BENCHMARK;
exports.skipCreate = skipCreate;
const testList = [];
if (process.env.GET_BENCHMARK_MATRIX) {
  process.on('exit', ({ exit }) => {
    console.log(JSON.stringify(testList));
    if (exit)
      process.exit();
  });
}
const fixture = (name, ...args) => {
  if (process.env.GET_BENCHMARK_MATRIX) {
    return;
  } else if (process.env.FIXTURES_ONLY) {
    tape(name, ...args);
  } else if (process.env.CURRENT_BENCHMARK) {
    tape(name, (t) => {
      t.skip();
      t.end();
    });
  }
};
exports.fixture = fixture;
const test = (name, ...args) => {
  if (process.env.GET_BENCHMARK_MATRIX) {
    testList.push(name);
  } else if (process.env.FIXTURES_ONLY) {
    tape(name, (t) => {
      t.skip();
      t.end();
    });
  } else if (process.env.CURRENT_BENCHMARK) {
    if (name.startsWith(process.env.CURRENT_BENCHMARK)) {
      tape.only(name, ...args);
    } else {
      tape(name, ...args);
    }
  } else {
    tape(name, ...args);
  }
};
exports.test = test;
