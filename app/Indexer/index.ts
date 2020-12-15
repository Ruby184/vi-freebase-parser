import { inject } from '@adonisjs/core/build/standalone'
import { ApplicationContract } from '@ioc:Adonis/Core/Application'
import { Client, ApiResponse, RequestParams } from '@elastic/elasticsearch'
import { createReadStream } from 'fs'
import { createGunzip } from 'zlib'
import { createInterface } from 'readline'

type Record = {
  mid: string
  title: string | null
  aliases: string[]
  types: string[]
}

@inject(['Adonis/Core/Application'])
export default class Indexer {
  // TODO: extract to config
  private client = new Client({ node: 'http://localhost:9200' })
  public indexName = 'freebase'
  
  constructor (protected app: ApplicationContract) {}

  public async indexFile (name: string): Promise<void> {
    await this.client.indices.delete(
      { index: this.indexName }
    )
    
    await this.client.indices.create({
      index: this.indexName,
      body: {
        mappings: {
          properties: {
            mid: { type: 'keyword' },
            title: {
              type: 'text',
              analyzer: 'english'
            },
            aliases: {
              type: 'text',
              analyzer: 'english'
            },
            types: {
              type: 'text',
              fields: {
                raw: {
                  type: 'keyword'
                }
              }
            }
          }
        }
      }
    })

    const fullPath = this.app.resourcesPath('output', name)
    const gunzipStream = createReadStream(fullPath).pipe(createGunzip())
    const rl = createInterface({ input: gunzipStream, crlfDelay: Infinity })
    const records: Record[] = []

    const indexRecords = async (refresh: boolean | 'wait_for' = false) => {
      const body = records.reduce(
        (acc, record) => acc.concat([{ index: { _index: this.indexName } }, record]),
        [] as any[]
      )

      records.length = 0

      const { body: response } = await this.client.bulk({ body, refresh })

      if (response.errors) {
        const erroredDocuments: any[] = []

        response.items.forEach((action, i) => {
          const operation = Object.keys(action)[0]

          if (action[operation].error) {
            erroredDocuments.push({
              status: action[operation].status,
              error: action[operation].error,
              operation: body[i * 2],
              document: body[i * 2 + 1]
            })
          }
        })

        throw erroredDocuments
      }
    }

    for await (const line of rl) {
      if (records.push(JSON.parse(line)) >= 300) {
        await indexRecords()
      }
    }

    if (records.length > 0) {
      await indexRecords('wait_for')
    }
  }

  async getTypesCount (size: number = 20000): Promise<ApiResponse['body']['hits']> {
    const params: RequestParams.Search = {
      index: this.indexName,
      body: {
        size: 0,
        aggs: {
          types: {
            terms: { field: 'types.raw', size }
          }
        }
      }
    }

    const { body } = await this.client.search(params)

    return body.aggregations.types.buckets
  }

  async getCountOfAliases (): Promise<number> {
    const params: RequestParams.Count = {
      index: this.indexName,
      body: {
        query: {
          exists : {
            field : 'aliases'
          }
        }
      }
    }

    const { body } = await this.client.count(params)

    return body.count
  }

  async search (query: string): Promise<any> {
    const { body } = await this.client.search({
      index: this.indexName,
      body: {
        query: {
          multi_match: {
            query,
            fields: ['title^2', 'aliases^3', 'types']
          }
        }
      }
    })
  
    return body.hits.hits.map((h) => h._source)
  }

  async getOfType (type: string): Promise<any> {
    const { body } = await this.client.search({
      index: this.indexName,
      body: {
        query: {
          term: {
            'types.raw': {
              value: type,
              boost: 1.0
            }
          }
        }
      }
    })
  
    return body.hits.hits.map((h) => h._source)
  }
}
