export async function timeit(f) {
  const start = process.hrtime();
  await f();
  const diff = process.hrtime(start);
  return (diff[0]*10e9 + diff[1]) / (10e6);
}