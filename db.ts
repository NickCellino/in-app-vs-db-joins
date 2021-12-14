import * as faker from 'faker';
import { Knex, knex } from 'knex';

const DATABASE_NAME = 'in_app_joins_experiment';

export function connectToDb(openDb: boolean) {
  return knex({
    client: 'pg',
    connection: {
      host: '127.0.0.1',
      port: 5432,
      database: openDb ? DATABASE_NAME : undefined
    },
    pool: { min: 1, max: 1 }
  });
}

export async function createDatabase(dbServer: Knex) {
  console.log('Creating database...');
  await dbServer.raw(`CREATE DATABASE ${DATABASE_NAME};`);
  console.log('Database created.');
}

export async function dropDatabase(dbServer: Knex, ignoreError: boolean = true) {
  console.log('Dropping database...');
  await dbServer.raw(`DROP DATABASE IF EXISTS ${DATABASE_NAME};`);
  console.log('Database dropped...');
}
export async function closeDb(db: Knex) {
  return db.destroy();
}

export async function initializeTables(db: Knex) {
  await db.schema.createTable('user', (users) => {
    users.increments('id');
    users.string('name');
  }).catch(() => { });

  await db.schema.createTable('post', (posts) => {
    posts.increments('id');
    posts.text('content');
    posts.integer('authorId').unsigned();
    posts.foreign('authorId').references('user.id').deferrable('deferred');
    posts.index('authorId');
  }).catch(() => { });
}

export async function populateDatabase(db: Knex, numPosts: number, numUsers: number) {
  let userInserts = [...Array(numUsers).keys()].map(() => {
    return {
      name: faker.name.findName()
    }
  });

  let users: { id?: number, name: string }[] = await db.batchInsert('user', userInserts).returning('*');

  let postInserts = [...Array(numPosts).keys()].map(() => {
    return {
      content: faker.lorem.words(200),
      authorId: faker.random.arrayElement(users).id
    }
  });

  await db.batchInsert('post', postInserts);
}