const elasticsearch = require('elasticsearch')

function createClient (host, log) {
  return new elasticsearch.Client({host, log})
 } 

function deleteIndex (client) {
  return client.indices.delete({ index: 'miujsag', ignoreUnavailable: true })
}

function deleteDocuments (client, q = '*') {
  return client.deleteByQuery({ index: 'miujsag', q, ignoreUnavailable: true })
}

function indexExists (client) {
  return client.indices.exists({ index: 'miujsag' })
}

function createIndex (client) {
  return client.indices.create({ index: 'miujsag' })
}

function putMapping (client) {
  return client.indices.putMapping({
    index: 'miujsag',
    type: 'article',
    body: {
      properties: {
        title: {
          type: 'text'
        },
        url: {
          type: 'text'
        },
        description: {
          type: 'text'
        },
        content: {
          type: 'text'
        },
        publishedAt: {
          type: 'date'
        },
        estimatedReadTime: {
          type: 'integer'
        },
        site: {
          type: 'nested',
          properties: {
            id: { type: 'string' },
            name: { type: 'string' },
            slug: { type: 'string' }
          }
        },
        category: {
          type: 'nested',
          properties: {
            id: { type: 'string' },
            name: { type: 'string' },
            slug: { type: 'string' }
          }
        }
      }
    }
  })
}

function destroy (client) {
  return deleteIndex(client)
    .then(deleteDocuments(client))
}

function init (client) {
  return indexExists(client)
    .then(exists => exists ? destroy(client) : '')
    .then(() => createIndex(client))
    .then(() => putMapping(client))
}

function index (client, document) {
  return client.index({
    index: 'miujsag',
    type: 'article',
    id: document.id,
    body: {
      title: document.title,
      description: document.description,
      content: document.content
    }
  })
}

function search (client, query, from = 0) {
  return client.search({
    index: 'miujsag',
    body: {
      from,
      query: {
        bool: {
          should: [
            {
              match_phrase: {
                content: query
              }
            },
            {
              fuzzy: {
                content: {
                  value: query,
                  fuzziness: 1,
                  prefix_length: 3
                }
              }
            }
          ]
        }
      },
      highlight: {
        fields: {
          content: {}
        }
      },
      _source: {
        exclude: ['content']
      }
    }
  })
}

module.exports = {
  createClient,
  index,
  search,
  init
}
