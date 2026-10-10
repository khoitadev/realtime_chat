module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  rootDir: '.',
  roots: ['<rootDir>/src'],
  testRegex: '.*\\.spec\\.ts$',
  setupFiles: ['<rootDir>/test/jest.env.ts'],
  testTimeout: 15000,
  // Spec files share one real Postgres test database (no mocking Prisma, per
  // spec) and each file's cleanDatabase() truncates the same tables. Running
  // spec files in parallel workers races their cleanup/creation against each
  // other, so they must run serially.
  maxWorkers: 1,
};
