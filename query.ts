import { writeFileSync } from "fs";
import { Knex } from "knex";

function extractQueryPlan(queryPlanResult): Promise<string> {
  return queryPlanResult.rows.map((row) => row['QUERY PLAN']).join('\n');
}

async function getQueryPerformance(db: Knex, query: Knex.QueryBuilder): Promise<string> {
  const result = await db.raw(`EXPLAIN ANALYZE ${query.toString()}`);
  return extractQueryPlan(result);
}

export async function queryUsingDbJoin(db: Knex): Promise<string> {
  const query = db.select(['post.id', 'user.id']).from('post').leftJoin('user', 'post.authorId', 'user.id');
  const result = await getQueryPerformance(db, query);
  return result;
}

export async function queryUsingInAppJoin(db: Knex) {
  const query1 = db.select(['post.id', 'post.authorId']).from('post');
  const result1 = await getQueryPerformance(db, query1);

  const posts = await query1;
  const authorIds = posts.map((post) => post.authorId);

  const query2 = db.select(['user.id']).from('user').whereIn('id', authorIds);
  const result2 = await getQueryPerformance(db, query2);

  return [result1, result2];
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
