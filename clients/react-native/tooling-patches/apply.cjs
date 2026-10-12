'use strict';

const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const {spawnSync} = require('child_process');

const appRoot = fs.realpathSync(process.cwd());

const repairs = {
  cli: {
    caller: '@react-native-community/cli-platform-android',
    callerVersion: '20.2.0',
    packageName: '@react-native-community/cli-config-android',
    version: '20.2.0',
    source: 'build/config/findComponentDescriptors.js',
    patch: 'cli-config-android-20.2.0.patch',
    preimage: '2cfe70a25818c4cd61fc8c122602f5363b8d57ffc1cf8c153f26fe68f1b8f047',
    postimage: '75ace921b876fb1164e2b80dda9045bfebf2972b9465da992bba5758452add94'
  },
  'stream-json': {
    caller: 'bunyamin',
    packageName: 'stream-json',
    version: '1.9.1',
    source: 'Assembler.js',
    patch: 'stream-json-1.9.1.patch',
    preimage: 'a341b21d86a9c005e5433eadb6c33d4708d611c8bc624ee383463c0fb6d2e32b',
    postimage: '51bf2168f777c8c7285dd521c32f0de636f363efc9baa80dbfb140fa55372203'
  }
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
    fail(`resolved target is outside the caller installation: ${target}`);
  }
  if (!relative.split(path.sep).includes('node_modules')) {
    fail(`resolved target is not in node_modules: ${target}`);
  }
}

function packageVersion(packageRoot, expected, label) {
  const value = JSON.parse(fs.readFileSync(path.join(packageRoot, 'package.json'), 'utf8')).version;
  if (value !== expected) {
    fail(`${label} version is ${value}, expected ${expected}`);
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

function applyRepair(name) {
  const repair = repairs[name];
  if (!repair) {
    fail(`unsupported repair ${name}`);
  }
  const caller = callerRoot(repair);
  assertInsideInstall(caller);
  if (repair.callerVersion) {
    packageVersion(caller, repair.callerVersion, repair.caller);
  }
  const target = targetRoot(repair, caller);
  assertInsideInstall(target);
  packageVersion(target, repair.version, repair.packageName);
  const source = path.join(target, repair.source);
  if (!fs.existsSync(source) || fs.realpathSync(source) !== source) {
    fail(`unexpected or missing source target: ${source}`);
  }
  const actual = hashFile(source);
  if (actual === repair.postimage) {
    process.stdout.write(`${repair.packageName} ${repair.version} already has the verified patch\n`);
    return;
  }
  if (actual !== repair.preimage) {
    fail(`${repair.packageName} source hash is ${actual}, expected ${repair.preimage}`);
  }
  const patchFile = path.join(__dirname, repair.patch);
  const result = spawnSync('patch', ['--batch', '--forward', '--fuzz=0', '-p1', '-d', target], {
    input: fs.readFileSync(patchFile),
    encoding: 'utf8',
    stdio: ['pipe', 'inherit', 'inherit']
  });
  if (result.error || result.status !== 0) {
    fail(`patch command failed for ${repair.packageName}`);
  }
  const postimage = hashFile(source);
  if (postimage !== repair.postimage) {
    fail(`${repair.packageName} postimage hash is ${postimage}, expected ${repair.postimage}`);
  }
  process.stdout.write(`applied verified ${repair.packageName} ${repair.version} patch\n`);
}

const name = process.argv[2] || 'cli';
if (!Object.prototype.hasOwnProperty.call(repairs, name)) {
  fail('usage: node tooling-patches/apply.cjs <cli|stream-json>');
}
applyRepair(name);
