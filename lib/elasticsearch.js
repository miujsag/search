const { Client } = require("@elastic/elasticsearch");
const { addWeeks } = require("date-fns");
const { prop } = require("ramda");

async function connectSimple(node) {
  return new Client({ node });
}

async function connect(node) {
  const client = new Client({ node });

  try {
    const doesExists = await client.indices.exists({ index: "miujsag" });

    if (!doesExists || (doesExists && doesExists.statusCode === 404)) {
      await client.indices.create({ index: "miujsag" });
    }

    return client;
  } catch (error) {
    console.log(error);
  }
}

async function index(client, article) {
  try {
    const exists = await client.search({
      index: "miujsag",
      body: {
        query: {
          constant_score: {
            filter: { term: { url: article.url } },
          },
        },
      },
    });

    if (!exists.body?.hits?.hits?.length) {
      const associations = {};

      if (article.Site) {
        associations.Site = {
          id: article.Site.id,
          name: article.Site.name,
          slug: article.Site.slug,
        };
      }

      if (article.Category) {
        associations.Category = {
          id: article.Category.id,
          name: article.Category.name,
        };
      }

      return client.index({
        index: "miujsag",
        id: article.id,
        body: {
          title: article.title,
          url: article.url,
          description: article.description,
          content: article.content
            ? article.content.replace(/\s\s+/g, " ")
            : "",
          published_at: article.published_at,
          estimated_read_time: article.estimated_read_time,
          image: article.image,
          ...associations,
        },
      });
    }
  } catch (error) {
    console.log(error);
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
        image: hit._source.image,
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

module.exports = { connect, connectSimple, index, search };
