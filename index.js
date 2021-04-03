const {
  connect,
  connectSimple,
  index,
  search,
} = require("./lib/elasticsearch");

module.exports = {
  search,
  connect,
  connectSimple,
  indexDocument: index,
};
