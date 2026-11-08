module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  rootDir: '.',
  roots: ['<rootDir>/src'],
  testRegex: '.*\\.spec\\.ts$',
  setupFiles: ['<rootDir>/test/jest.env.ts'],
  testTimeout: 20000,
  // Specs share one RabbitMQ vhost and one Redis database; run files serially.
  maxWorkers: 1,
};
