import { closeDb, connectToDb, createDatabase, dropDatabase, initializeTables, populateDatabase } from './db';
import { parseQueryTime, queryUsingDbJoin, queryUsingInAppJoin } from './query';
import { writeFileSync } from 'fs';

type TestScenarioResult = {
  method: 'in_app' | 'db',
  numUsers: number,
  numPosts: number,
  repetitions: {
    results: { planningTime: number, executionTime: number }[]
  }[]
}

export async function testScenario(db, numPosts, numUsers, numRepetitions): Promise<TestScenarioResult[]> {
  await populateDatabase(db, numPosts, numUsers);

  let dbJoinReps: { planningTime: number, executionTime: number }[] = [];
  for (const _ of [...Array(numRepetitions)].keys()) {
    const dbJoinResult = await queryUsingDbJoin(db);
    dbJoinReps.push(parseQueryTime(dbJoinResult));
  };

  let inAppJoinReps: {planningTime: number, executionTime: number}[][] = [];
  for (const _ of [...Array(numRepetitions)].keys()) {
    const inAppJoinResults = await queryUsingInAppJoin(db);
    inAppJoinReps.push(inAppJoinResults.map((result) => parseQueryTime(result)));
  };

  return [
    {
      method: 'db',
      numPosts: numPosts,
      numUsers: numUsers,
      repetitions: dbJoinReps.map(rep => ({
        results: [rep]
      }))
    },
    {
      method: 'in_app',
      numPosts: numPosts,
      numUsers: numUsers,
      repetitions: inAppJoinReps.map(rep => ({ results: rep }))
    }
  ]
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

  writeFileSync('postsOutputs.json', JSON.stringify(scenarioResults));
}

export async function runNumJoinsExperiment() {
  let scenarios = [
    // { numPosts: 1000, numUsers: 10000 },
    // { numPosts: 10000, numUsers: 10000 },
    { numPosts: 100000, numUsers: 10000 },
    // { numPosts: 1000000, numUsers: 10000 },
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
    console.log(results);

    await closeDb(db);
    await closeDb(dbServer);
    scenarioResults = scenarioResults.concat(results);
  };

  writeFileSync('numJoinsOutputs.json', JSON.stringify(scenarioResults));
}