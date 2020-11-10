import { inject } from '@adonisjs/core/build/standalone'
import { ApplicationContract } from '@ioc:Adonis/Core/Application'
import { Client } from '@elastic/elasticsearch'
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
    const fullPath = this.app.resourcesPath('output', name)
    const gunzipStream = createReadStream(fullPath).pipe(createGunzip())
    const rl = createInterface({ input: gunzipStream, crlfDelay: Infinity })

    await this.client.indices.delete({ index: this.indexName })
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
    }, { ignore: [400] })

    const records: Record[] = []

    for await (const line of rl) {
      if (records.push(JSON.parse(line)) >= 100) {
        const body = records.reduce(
          (acc, record) => acc.concat([{ index: { _index: this.indexName } }, record]),
          [] as any[]
        )

        records.length = 0

        const { body: response } = await this.client.bulk({ body, refresh: true })

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

        break
      }
    }

    const { body } = await this.client.search({
      index: this.indexName,
      body: {
        query: {
          match: {
            title: 'claudia'
          }
        }
      }
    })
  
    console.log(body.hits.hits)
  }
}
