import { runNumJoinsExperiment, runPostsExperiment } from './runExperiment';

async function main() {
  await runPostsExperiment();
  // await runNumJoinsExperiment();
}

main()