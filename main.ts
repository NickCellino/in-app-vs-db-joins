import { program } from 'commander';

import { closeDb, connectToDb, createDatabase, dropDatabase, initializeTables, populateDatabase } from "./db";
import { runPostsExperiment } from './runExperiment';

program.option('-d --drop-db', 'Drop the existing database before running the test.')
program.parse()

async function main(options) {
  let db = connectToDb(true);
  if (options.dropDb) {
    let dbServer = connectToDb(false);
    await dropDatabase(dbServer);
    await createDatabase(dbServer);
    await closeDb(dbServer);
    await initializeTables(db);
  }
  await runPostsExperiment();
  await closeDb(db);
}

main(program.opts());