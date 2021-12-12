import { closeDb, connectToDb, createDatabase, dropDatabase, initializeTables, populateDatabase } from './db';
import { parseQueryTime, queryUsingDbJoin, queryUsingInAppJoin } from './query';
import { writeFileSync } from 'fs';
import { timeit } from './timing';

type TestScenarioResult = {
  method: 'in_app' | 'db',
  numUsers: number,
  numPosts: number,
  timeMs: number
}

export async function testScenario(db, numPosts, numUsers, numRepetitions): Promise<TestScenarioResult[]> {
  await populateDatabase(db, numPosts, numUsers);

  let dbJoinReps: number[] = [];
  for (const _ of [...Array(numRepetitions)].keys()) {
    const queryTime = await timeit(async () => queryUsingDbJoin(db));
    dbJoinReps.push(queryTime);
  };

  let inAppJoinReps: number[] = [];
  for (const _ of [...Array(numRepetitions)].keys()) {
    const functionTime = await timeit(async () => queryUsingInAppJoin(db));
    inAppJoinReps.push(functionTime);
  };

  const dbJoinData: TestScenarioResult[] = dbJoinReps.map(rep => (
    { method: 'db', numPosts: numPosts, numUsers: numUsers, timeMs: rep }
  ));
  const inAppJoinData: TestScenarioResult[] = inAppJoinReps.map(rep => (
    { method: 'in_app', numPosts: numPosts, numUsers: numUsers, timeMs: rep }
  ));

  return dbJoinData.concat(inAppJoinData);
}


export async function runPostsExperiment() {
  let scenarios = [
    { numPosts: 1000, numUsers: 1000 },
    { numPosts: 1000, numUsers: 10000 },
    { numPosts: 1000, numUsers: 100000 },
    { numPosts: 1000, numUsers: 1000000 },
  ];

  let scenarioResults: TestScenarioResult[] = [];

  for (const scenario of scenarios) {
    console.log('testing scenario: ', scenario);
    let db = connectToDb(true);
    let dbServer = connectToDb(false);
    await dropDatabase(dbServer);
    await createDatabase(dbServer);
    await initializeTables(db);
    console.log('database has been initialized for scenario: ', scenario);

    let results = await testScenario(db, scenario.numPosts, scenario.numUsers, 3);
    console.log('finished gathering results for scenario: ', scenario);

    await closeDb(db);
    await closeDb(dbServer);
    scenarioResults = scenarioResults.concat(results);
  };

  console.log(JSON.stringify(scenarioResults));
  writeFileSync('postsOutputs.json', JSON.stringify(scenarioResults));
}

export async function runNumJoinsExperiment() {
  let scenarios = [
    { numPosts: 1000, numUsers: 10000 },
    { numPosts: 2000, numUsers: 10000 },
    { numPosts: 3000, numUsers: 10000 },
    { numPosts: 4000, numUsers: 10000 },
    { numPosts: 5000, numUsers: 10000 },
    { numPosts: 6000, numUsers: 10000 },
    { numPosts: 7000, numUsers: 10000 },
    { numPosts: 8000, numUsers: 10000 },
    { numPosts: 9000, numUsers: 10000 },
    { numPosts: 10000, numUsers: 10000 },
    { numPosts: 20000, numUsers: 10000 },
    { numPosts: 30000, numUsers: 10000 },
    { numPosts: 40000, numUsers: 10000 },
    { numPosts: 50000, numUsers: 10000 },
  ];

  let scenarioResults: TestScenarioResult[] = [];

  for (const scenario of scenarios) {
    console.log('testing scenario: ', scenario);
    let db = connectToDb(true);
    let dbServer = connectToDb(false);
    await dropDatabase(dbServer);
    await createDatabase(dbServer);
    await initializeTables(db);
    console.log('database has been initialized for scenario: ', scenario);

    let results = await testScenario(db, scenario.numPosts, scenario.numUsers, 3);
    console.log('finished gathering results for scenario: ', scenario);

    await closeDb(db);
    await closeDb(dbServer);
    scenarioResults = scenarioResults.concat(results);
  };

  writeFileSync('numJoinsOutputs.json', JSON.stringify(scenarioResults));
}