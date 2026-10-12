'use strict';

const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const {spawnSync} = require('child_process');

const appRoot = fs.realpathSync(process.cwd());

const repairs = {
  cli: [{
    caller: '@react-native-community/cli-platform-android',
    callerVersion: '20.2.0',
    packageName: '@react-native-community/cli-config-android',
    version: '20.2.0',
    patch: 'cli-config-android-20.2.0.patch',
    sources: [{
      path: 'build/config/findComponentDescriptors.js',
      preimage: '2cfe70a25818c4cd61fc8c122602f5363b8d57ffc1cf8c153f26fe68f1b8f047',
      postimage: '75ace921b876fb1164e2b80dda9045bfebf2972b9465da992bba5758452add94'
    }]
  }, {
    caller: '@react-native-community/cli',
    callerVersion: '20.2.0',
    packageName: '@react-native-community/cli-clean',
    version: '20.2.0',
    patch: 'cli-clean-20.2.0.patch',
    sources: [{
      path: 'build/clean.js',
      preimage: 'f46f188859df1d616676d5cb246ed0b96b8f5e71868c5df914d31830acdde4be',
      postimage: '9aea3a5112e0432ec78f5716d91ceac56d35e2b24eb5c45f4f28d254b9eaf9c2'
    }]
  }],
  'stream-json': [{
    caller: 'bunyamin',
    callerVersion: '1.6.3',
    packageName: 'stream-json',
    version: '1.9.1',
    patch: 'stream-json-1.9.1.patch',
    sources: [{
      path: 'Assembler.js',
      preimage: 'a341b21d86a9c005e5433eadb6c33d4708d611c8bc624ee383463c0fb6d2e32b',
      postimage: '51bf2168f777c8c7285dd521c32f0de636f363efc9baa80dbfb140fa55372203'
    }]
  }],
  bob: [{
    caller: 'react-native-builder-bob',
    callerVersion: '0.40.18',
    packageName: 'react-native-builder-bob',
    version: '0.40.18',
    patch: 'react-native-builder-bob-0.40.18.patch',
    sources: [{
      path: 'lib/targets/module.js',
      preimage: '1ef8ee20f2767347348e86de7f421358b7705623ae04a9b952fabc61f365ede6',
      postimage: 'd1236a54d5fb13aeb3bdab4b6a5b398e2bd6da7862ef4bed3899751fd1104e05'
    }, {
      path: 'lib/targets/typescript.js',
      preimage: '451fe8050e20bc1c4724564b2c33fb819f815848f5ad2eb529be5915bf62f2bb',
      postimage: 'acf316d47964c16b47c9e1fcd7af59438fb51237cb4fb5446f3f1c15d33b681a'
    }]
  }]
};

function fail(message) {
  throw new Error(`Synchro tooling patch: ${message}`);
}

function resolvedPackageRoot(request, paths) {
  const packageJson = require.resolve(`${request}/package.json`, {paths});
  return fs.realpathSync(path.dirname(packageJson));
}

function assertInsideInstall(target) {
  const relative = path.relative(appRoot, target);
  if (relative === '' || relative.startsWith('..') || path.isAbsolute(relative)) {
    fail('resolved target is outside the caller installation');
  }
  if (!relative.split(path.sep).includes('node_modules')) {
    fail('resolved target is not in node_modules');
  }
}

function packageIdentity(packageRoot, expectedName, expectedVersion) {
  const manifest = JSON.parse(fs.readFileSync(path.join(packageRoot, 'package.json'), 'utf8'));
  if (manifest.name !== expectedName) {
    fail('unexpected package identity');
  }
  if (manifest.version !== expectedVersion) {
    fail('unexpected package version');
  }
}

function hashFile(file) {
  return crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex');
}

function callerRoot(repair) {
  if (repair.caller === 'bunyamin') {
    const entry = require.resolve('bunyamin', {paths: [appRoot]});
    let directory = path.dirname(fs.realpathSync(entry));
    while (directory !== path.dirname(directory)) {
      const manifest = path.join(directory, 'package.json');
      if (fs.existsSync(manifest) && JSON.parse(fs.readFileSync(manifest, 'utf8')).name === 'bunyamin') {
        return directory;
      }
      directory = path.dirname(directory);
    }
    fail('could not locate the resolved bunyamin package root');
  }
  return resolvedPackageRoot(repair.caller, [appRoot]);
}

function targetRoot(repair, caller) {
  if (repair.caller === 'bunyamin') {
    const source = require.resolve('stream-json/Assembler.js', {paths: [caller]});
    return fs.realpathSync(path.dirname(source));
  }
  return resolvedPackageRoot(repair.packageName, [caller]);
}

function applyRepair(repair) {
  const caller = callerRoot(repair);
  assertInsideInstall(caller);
  packageIdentity(caller, repair.caller, repair.callerVersion);
  const target = targetRoot(repair, caller);
  assertInsideInstall(target);
  packageIdentity(target, repair.packageName, repair.version);
  const sources = repair.sources.map(source => {
    const file = path.join(target, source.path);
    if (!fs.existsSync(file) || fs.realpathSync(file) !== file) {
      fail('unexpected or missing source target');
    }
    return {file, source};
  });
  if (sources.every(({file, source}) => hashFile(file) === source.postimage)) {
    process.stdout.write(`${repair.packageName} ${repair.version} already has the verified patch\n`);
    return;
  }
  if (!sources.every(({file, source}) => hashFile(file) === source.preimage)) {
    fail('unexpected source hash');
  }
  const patchFile = path.join(__dirname, repair.patch);
  const result = spawnSync('patch', ['--batch', '--forward', '--fuzz=0', '-p1', '-d', target], {
    input: fs.readFileSync(patchFile),
    encoding: 'utf8',
    stdio: ['pipe', 'inherit', 'inherit']
  });
  if (result.error || result.status !== 0) {
    fail('patch command failed');
  }
  if (!sources.every(({file, source}) => hashFile(file) === source.postimage)) {
    fail('postimage verification failed');
  }
  process.stdout.write(`applied verified ${repair.packageName} ${repair.version} patch\n`);
}

const name = process.argv[2] || 'cli';
if (!Object.prototype.hasOwnProperty.call(repairs, name)) {
  fail('unsupported repair');
}
for (const repair of repairs[name]) {
  applyRepair(repair);
}
