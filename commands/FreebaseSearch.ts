import { BaseCommand, args, flags, inject } from '@adonisjs/core/build/standalone'

const QUERY_TYPES = [
	'fulltext',
	'types'
] as const

export default class FreebaseSearch extends BaseCommand {

	/**
	 * Command Name is used to run the command
	 */
  public static commandName = 'freebase:search'

	/**
	 * Command Name is displayed in the "help" output
	 */
	public static description = ''
	
	public static settings = {
		/**
		 * If your command relies on the application code, then you must set the following
		 * property true.
		 */
  	loadApp: true,

		/**
		 * AdonisJS forcefully kills the process after running the ace command. However, if you
		 * set the following property to true if your command needs a long running process.
		 */
  	stayAlive: false,
	}

	@flags.string({
		description: 'Type of query to perform',
		async defaultValue (command) {
			return command.prompt.choice('Select type of query to perform', QUERY_TYPES)
		}
	})
	public type: typeof QUERY_TYPES[number]

	@inject(['App/Indexer'])
  public async run (indexer) {
		switch (this.type) {
			case 'fulltext':
				await this.fulltextSearch(indexer)
			break

			case 'types':
				await this.typesQuery(indexer)
			break
		}
	}
	
	private async fulltextSearch (indexer): Promise<void> {
		const query = await this.prompt.ask('Enter search query: ')
		const results = await indexer.search(query)

		return this.renderResultsTable(results)
	}

	private async typesQuery (indexer): Promise<void> {
		const types = await indexer.getTypesCount()
		const selected = await this.prompt.autocomplete('Enter type: ', types.map((t) => t.key))
		const results = await indexer.getOfType(selected)

		return this.renderResultsTable(results)
	}

	private renderResultsTable (results: any): void {
		const table = this.ui.table()

		table.head(['mid', 'Title', 'Aliases', 'Types'])

		for (const { mid, title, aliases, types } of results) {
			table.row([mid, title, aliases.join('\n'), types.join('\n')])
		}

		table.render()
	}
}
