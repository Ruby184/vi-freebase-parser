import { ApplicationContract } from '@ioc:Adonis/Core/Application'
import { inject } from '@adonisjs/fold'
import { createReadStream } from 'fs'
import { createGunzip } from 'zlib'
import { createInterface } from 'readline'

@inject(['Adonis/Core/Application'])
export default class Parser {
  constructor (protected app: ApplicationContract) {}

  async parse (path: string) {
    const fullPath = this.app.resourcesPath('freebase', path)
    const gunzipStream = createReadStream(fullPath).pipe(createGunzip())
    const rl = createInterface({ input: gunzipStream, crlfDelay: Infinity })
    // const regex = /<(?<subject>.+)>\s+<(?<predicate>.+)>\s+"(?<object>.+)"@en\s+\./
    const regex = /^<(.+)>\s+<(.+)>\s+((?:"(?<text>.+)"@en)|(?:<.+\/(?<object>.+)>))\s+\.$/
    const result = {}

    for await (const line of rl) {
      const match = line.match(regex)

      if (match !== null) {
        const subject = match[1]

        switch (match[2]) {
          case 'http://rdf.freebase.com/ns/common.topic.alias':
            result[subject] = result[subject] || { title: null, aliases: [], types: [] }
            result[subject].aliases.push(match.groups!.text.replace(/\\n/g, '\n'))
          break

          case 'http://rdf.freebase.com/ns/type.object.name':
            result[subject] = result[subject] || { title: null, aliases: [], types: [] }
            result[subject].title = match.groups!.text.replace(/\\n/g, '\n')
          break

          case 'http://rdf.freebase.com/ns/type.object.type':
            result[subject] = result[subject] || { title: null, aliases: [], types: [] }
            result[subject].types.push(match.groups!.object)
          break
        }
      }
    }

    return result
  }
}