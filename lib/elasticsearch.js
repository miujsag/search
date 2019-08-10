const elasticsearch = require('elasticsearch')
const {addWeeks} = require('date-fns')
const {prop} = require('ramda')

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

function index (client, document, site, category) {
  return client.index({
    index: 'miujsag',
    type: 'article',
    id: document.id,
    body: {
      title: document.title,
      url: document.url,
      description: document.description,
      content: document.content ? document.content.replace(/\s\s+/g, ' ') : '',
      publishedAt: document.publishedAt,
      estimatedReadTime: document.estimatedReadTime,
      site: {
        id: site.id,
        name: site.name,
        slug: site.slug
      },
      category: {
        id: category.id,
        name: category.name
      }
    }
  })
}

function formatResult (results) {
  const resultObject = {
    total: 0,
    articles: []
  }

  const hits = prop('hits', prop('hits', results))

  if (hits) {
    resultObject.articles = hits.map(hit => {
      return {
        title: hit._source.title,
        url: hit._source.url,
        description: hit._source.description,
        estimatedReadTime: hit._source.estimatedReadTime,
        publishedAt: hit._source.publishedAt,
        site: {
          name: hit._source.site.name,
          slug: hit._source.site.slug
        }
      }
    })

    resultObject.total = results.hits.total
  }

  return resultObject
}

function search (client, query, sites, categories, from = addWeeks(new Date(), - 1), until = new Date(), skip = 0, sort = '_score') {
  return client.search({
    index: 'miujsag',
    body: {
      from: skip,
      size: 20,
      query: {
        bool: {
          must: [
            {
              bool: {
                should: [
                  {
                    multi_match: {
                      query,
                      fields: ['title', 'description', 'content'],
                      fuzziness: 1,
                      prefix_length: 2
                    }
                  }
                ]
              }
            }            
          ],
          filter: {
            bool: {
              must: [
                {
                  terms: {
                    'site.id': sites
                  }
                },
                {
                  terms: {
                    'category.id': categories
                  }
                },
                {
                  range: {
                    publishedAt: {
                      lte: until
                    }
                  }
                },
                {
                  range: {
                    publishedAt: {
                      gte: from
                    }
                  }
                }
              ]
            }   
          }
        }
      },
      highlight: {
        fields: {
          content: {}
        },
        number_of_fragments: 1
      },
      _source: {
        exclude: ['content']
      },
      sort: {
        [sort]: {
          order: 'desc'
        }
      }
    }
  }).then(formatResult)
}

module.exports = {
  createClient,
  index,
  search,
  init
}
