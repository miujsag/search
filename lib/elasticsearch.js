const elasticsearch = require("elasticsearch");
const { addWeeks } = require("date-fns");
const { prop } = require("ramda");

function createClient(host, log) {
  return new elasticsearch.Client({ host, log });
}

function indexExists(client) {
  return client.indices.exists({ index: "miujsag" });
}

function createIndex(client) {
  return client.indices.create({ index: "miujsag" });
}

async function init(client) {
  try {
    const doesExists = await indexExists(client);

    if (!doesExists) {
      await createIndex(client);
    }

    return client;
  } catch (error) {
    return new Error(error);
  }
}

async function index(client, document, site, category) {
  try {
    const exists = await client.search({
      index: "miujsag",
      body: {
        query: {
          bool: {
            must: { match: { title: document.url } },
          },
        },
      },
    });

    if (!exists.hits || !exists.hits.hits || exists.hits.hits.length < 1) {
      return client.index({
        index: "miujsag",
        type: "article",
        id: document.id,
        body: {
          title: document.title,
          url: document.url,
          description: document.description,
          content: document.content
            ? document.content.replace(/\s\s+/g, " ")
            : "",
          published_at: document.published_at,
          estimated_read_time: document.estimated_read_time,
          site: {
            id: site.id,
            name: site.name,
            slug: site.slug,
          },
          category: {
            id: category.id,
            name: category.name,
          },
        },
      });
    }
  } catch (error) {
    return new Error(error);
  }
}

function formatResult(results) {
  const resultObject = {
    total: 0,
    articles: [],
  };

  const hits = prop("hits", prop("hits", results));

  if (hits) {
    resultObject.articles = hits.map((hit) => {
      return {
        title: hit._source.title,
        url: hit._source.url,
        description: hit._source.description,
        estimated_read_time: hit._source.estimated_read_time,
        published_at: hit._source.published_at,
        Site: {
          name: hit._source.site.name,
          slug: hit._source.site.slug,
        },
        highlights: prop("content", hit.highlight),
      };
    });

    resultObject.total = results.hits.total;
  }

  return resultObject;
}

function search(
  client,
  query,
  sites,
  categories,
  from = addWeeks(new Date(), -1),
  until = new Date(),
  skip = 0,
  sort = "_score"
) {
  const orderBy = sort === "date" ? "published_at" : "_score";

  return client
    .search({
      index: "miujsag",
      body: {
        from: skip,
        size: 20,
        query: {
          bool: {
            must: [
              {
                multi_match: {
                  query,
                  fields: ["title", "description", "content"],
                  fuzziness: 1,
                  prefix_length: 2,
                },
              },
              {
                terms: {
                  "site.id": sites,
                },
              },
              {
                terms: {
                  "category.id": categories,
                },
              },
            ],
            filter: [
              {
                range: {
                  published_at: {
                    lte: until,
                  },
                },
              },
              {
                range: {
                  published_at: {
                    gte: from,
                  },
                },
              },
            ],
          },
        },
        highlight: {
          fields: {
            content: {},
          },
          number_of_fragments: 1,
        },
        _source: {
          exclude: ["content"],
        },
        sort: {
          [orderBy]: {
            order: "desc",
          },
        },
      },
    })
    .then(formatResult);
}

module.exports = {
  createClient,
  index,
  search,
  init,
};
