import { program } from 'commander';
import { connectToDb, createDatabase, dropDatabase, initializeTables, populateDatabase } from './db';
import { queryUsingDbJoin, queryUsingInAppJoin } from './query';
import { timeit } from './timing';

program.requiredOption('-p --posts <value>', 'Number of posts to create');
program.requiredOption('-u --users <value>', 'Number of users to create');
program.requiredOption('-m --method <value>', 'Method to use (db or app)');


const queryFunctions = {
  db: (db) => queryUsingDbJoin(db),
  app: (db) => queryUsingInAppJoin(db)
}

async function runExperiment({ posts, users, method }: { posts: number, users: number, method: 'db' | 'app' }) {
  posts = Number(posts);
  users = Number(users);

  const dbServer = connectToDb(false);
  await dropDatabase(dbServer);
  await createDatabase(dbServer);
  await dbServer.destroy();

  const db = connectToDb(true);
  await initializeTables(db);
  const populateDbTime = await timeit(async () => populateDatabase(db, posts, users));
  console.log(`Took ${populateDbTime} ms to populate the DB.`)

  const result = await timeit(async () => queryFunctions[method](db));

  await db.destroy();

  return result;
}

async function main() {
  program.parse();
  const result = await runExperiment(program.opts());
  console.log(`Experiment took ${result} ms`)
}

main()