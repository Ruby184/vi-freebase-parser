import LRU from 'lru-cache'
import { inject } from '@adonisjs/core/build/standalone'
import { ApplicationContract } from '@ioc:Adonis/Core/Application'
import { createReadStream, createWriteStream } from 'fs'
import { createGunzip, createGzip } from 'zlib'
import { createInterface } from 'readline'
import { Transform, TransformCallback, finished } from 'stream'

class RecordTransform extends Transform {
  constructor () {
    super({
      writableObjectMode: true,
      encoding: 'utf8'
    })
  }
  
  public _transform (chunk: Record, _: BufferEncoding, callback: TransformCallback): void {
    try {
      callback(null, JSON.stringify(chunk) + '\n')
    } catch (err) {
      callback(err)
    }
  }
}

type Record = {
  mid: string
  title: string | null
  aliases: string[]
  types: string[]
}

@inject(['Adonis/Core/Application'])
export default class Parser {
  private processed: Set<string> = new Set()

  private ws = new RecordTransform()
  private drain = Promise.resolve()

  private cache = new LRU({
    max: 100,
    dispose: (key, val) => {
      if (this.processed.has(key)) {
        console.log('Already saved id found', key)
      } else {
        this.processed.add(key)
      }

      if (!this.ws.write(val)) {
        this.drain = new Promise((resolve) => {
          this.ws.once('drain', resolve)
        })
      }
    }
  })

  private regex = /^<.+\/m\.([a-z0-9_]+)>\s+<(.+)>\s+((?:"(?<text>.+)"@en)|(?:<.+\/(?<type>.+)>))\s+\.$/
  
  constructor (protected app: ApplicationContract) {}

  private getRecord (mid: string): Record {
    if (!this.cache.has(mid)) {
      this.cache.set(mid, { mid, title: null, aliases: [], types: [] })
    }

    return this.cache.get(mid)
  }

  async parse (path: string) {
    const fullPath = this.app.resourcesPath('freebase', path)
    const gunzipStream = createReadStream(fullPath).pipe(createGunzip())
    const rl = createInterface({ input: gunzipStream, crlfDelay: Infinity })

    const output = this.ws.pipe(createGzip()).pipe(
      createWriteStream(this.app.resourcesPath('output', path))
    )

    for await (const line of rl) {
      const match = this.regex.exec(line)

      if (match !== null) {
        const mid = match[1]
        const escapeRegex = /\\([n"\\])/g
        const replacer = (_, c) => c === 'n' ? '\n': c

        switch (match[2]) {
          case 'http://rdf.freebase.com/ns/common.topic.alias':
            const { aliases } = this.getRecord(mid)
            aliases.push(match.groups!.text.replace(escapeRegex, replacer))
          break

          case 'http://rdf.freebase.com/ns/type.object.name':
            const record = this.getRecord(mid)
            record.title = match.groups!.text.replace(escapeRegex, replacer)
          break

          case 'http://rdf.freebase.com/ns/type.object.type':
            this.getRecord(mid).types.push(match.groups!.type)
          break
        }
      }

      await this.drain
    }

    this.cache.reset()
    this.ws.end()

    return new Promise((resolve, reject) => {
      const cleanup = finished(output, (err) => {
        cleanup()

        if (err) {
          reject(err)
        } else {
          resolve()
        }
      })
    })
  }
}