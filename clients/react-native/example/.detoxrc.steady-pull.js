const config = require('./.detoxrc');

module.exports = {
  ...config,
  testRunner: {
    ...config.testRunner,
    args: {
      ...config.testRunner.args,
      _: ['e2e/steady-pull.test.ts'],
    },
  },
};
