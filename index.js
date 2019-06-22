const {createClient, index, init, search} = require('./lib/elasticsearch')

module.exports = {
  search,
  setup: init,
  createSearchClient: createClient,
  indexDocument: index
}
