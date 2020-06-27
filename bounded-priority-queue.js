const binary = require('bipf')

module.exports = function (limit) {
  var sorted = [] // { seq, value, seekKey }

  function sort(a, b) {
    return binary.compare(a.value, a.seekKey,
			  b.value, b.seekKey)
  }
  
  return {
    sorted,

    add: function(seq, value, seekKey) {
      if (sorted.length < limit) {
	sorted.push({ seq, value, seekKey })
	sorted.sort(sort)
      }
      else {
	if (binary.compare(value, seekKey,
			   sorted[0].value, sorted[0].seekKey)) {
	  sorted[0] = { seq, value, seekKey }
	  sorted.sort(sort)
	}
      }
    }
  }
}
