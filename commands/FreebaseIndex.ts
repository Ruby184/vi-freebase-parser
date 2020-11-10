import { BaseCommand, args, inject } from '@adonisjs/core/build/standalone'

export default class FreebaseIndex extends BaseCommand {

	/**
	 * Command Name is used to run the command
	 */
  public static commandName = 'freebase:index'

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
	
	@args.string({ description: 'File path of gziped freebase output' })
	public path: string

	@inject(['App/Indexer'])
  public async run (indexer) {
    await indexer.indexFile(this.path)
  }
}
