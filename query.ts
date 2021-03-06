import { writeFileSync } from "fs";
import { Knex } from "knex";

function extractQueryPlan(queryPlanResult): Promise<string> {
  return queryPlanResult.rows.map((row) => row['QUERY PLAN']).join('\n');
}

async function getQueryPerformance(db: Knex, query: Knex.QueryBuilder): Promise<string> {
  const result = await db.raw(`EXPLAIN ANALYZE ${query.toString()}`);
  return extractQueryPlan(result);
}

export async function queryUsingDbJoin(db: Knex): Promise<any> {
  const results = await db.select(['post.id as postId', 'user.name as authorName']).from('post').leftJoin('user', 'post.authorId', 'user.id');
  return results;
}

export async function queryUsingInAppJoin(db: Knex) {
  const posts = await db.select(['post.id', 'post.authorId']).from('post');

  let authorIds = posts.map((post) => post.authorId);

  const authorsMap = {};

  const batchSize = 5000;
  while (authorIds.length) {
    let authorIdsBatch = authorIds.slice(0, batchSize);

    const authors = await db.select(['user.id', 'user.name']).from('user').whereIn('id', authorIdsBatch);
    authors.forEach((author) => {
      authorsMap[author.id] = author;
    });

    authorIds = authorIds.slice(batchSize);
  }

  const results = posts.map((post) => {
    return {
      'postId': post.id,
      'authorName': authorsMap[post.authorId].name
    }
  });
  return results;
}

export function parseQueryTime(explainAnalyzeOutput: string): { planningTime: number, executionTime: number } {
  let planningTimeRegex = /Planning Time: (?<planningTime>[\d\.]+) ms/;
  let executionTimeRegex = /Execution Time: (?<executionTime>[\d\.]+) ms/;
  let planningTime = planningTimeRegex.exec(explainAnalyzeOutput)?.groups?.planningTime;
  let executionTime = executionTimeRegex.exec(explainAnalyzeOutput)?.groups?.executionTime;
  return {
    planningTime: Number(planningTime),
    executionTime: Number(executionTime)
  }
}
