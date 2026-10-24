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
      postimage: '16686f186b97a7a83fa6fc761db191a623d0f5697acb45b8df907e343c7629fa'
    }, {
      path: 'lib/targets/typescript.js',
      preimage: '451fe8050e20bc1c4724564b2c33fb819f815848f5ad2eb529be5915bf62f2bb',
      postimage: 'd22c16e9d4ebe97b15a3b734b70e1060a5526b907cad2b1ec3bb1004d4ed0675'
    }]
  }],
  rngp: [{
    caller: 'react-native',
    callerVersion: '0.88.0',
    packageName: '@react-native/gradle-plugin',
    version: '0.88.0',
    patch: 'react-native-gradle-plugin-0.88.0-kotlin-2.4.21.patch',
    sources: [{
      path: 'gradle/libs.versions.toml',
      preimage: 'c7a028cdee90f490de9493ca9de92b61b994844836161871091c340c568f109e',
      postimage: '44a27d41062a7c01889fac1bc7666f8235ee14060de8588d66660830f8484c7d'
    }, {
      path: 'react-native-gradle-plugin/build.gradle.kts',
      preimage: '44bcf53d885c52ef9988b7147e8e522563eaf519144109f40390fc7601a6469e',
      postimage: 'cced8bd5897896d6f00149e4704b51130e8b9468f39ab4247f9aca0f2ddcc864'
    }, {
      path: 'settings-plugin/build.gradle.kts',
      preimage: '326cc1d071e500711a12134d0577059929ae3e6960e2d1bfb10e0cffea8832f2',
      postimage: '3af9b7ea340523e24e3492db32da30943dd840c3f2e656ffdc72b7a37129a3d5'
    }, {
      path: 'shared-testutil/build.gradle.kts',
      preimage: '40950ae3b3bf68b35515f022f2819c98def11f0aee67ade7ed64f04c6f033cca',
      postimage: '2db5af783296917c76c1f410ead33ed555310917a79b3b005ee996f1ac23a6d6'
    }, {
      path: 'shared/build.gradle.kts',
      preimage: '7069a997b96e4c8a733a62e00d557730ab32bde6a5dc54dbd18d86d2bc302d09',
      postimage: '5d8d113c7849a448d1abafb8fcdb545f389133fcd3fb6c68c12089de53e24c6c'
    }]
  }, {
    caller: 'react-native',
    callerVersion: '0.82.1',
    packageName: '@react-native/gradle-plugin',
    version: '0.82.1',
    patch: 'react-native-gradle-plugin-0.82.1-kotlin-2.4.21.patch',
    sources: [{
      path: 'gradle/libs.versions.toml',
      preimage: 'eea932505dd9efd2d660d8d10efddbe84b74ed47456d995099af7ab5d97d6802',
      postimage: '8a30e112ad18be9bc95f509d1cc42affd09e23dd2cfe22677cd7c7703628247f'
    }, {
      path: 'react-native-gradle-plugin/build.gradle.kts',
      preimage: 'b45b3f39996d9db2b54c68cca073063c605d313aae548f633b6a00fc6df556b0',
      postimage: 'f0b609f0c29dbd6e90de935d3d6690b5cc6dd61f314345ccb662a0baf11a5fcc'
    }, {
      path: 'settings-plugin/build.gradle.kts',
      preimage: '18d28065072ad8c93656deadfbcff095a05fd6a58e25d047eb69d839e347aa97',
      postimage: '95b2086c29bd771fd9e52d23c931897852d6669138f0939a1b26a14f67c9a41e'
    }, {
      path: 'shared-testutil/build.gradle.kts',
      preimage: '8cb397c7d640a836c2edce904cf2fbf90c069a2b228b539c0243e3d7997854bf',
      postimage: '9af52f0cdf6f76a713598db2da6c7679390625028a23a0946c27d9df580d4bdc'
    }, {
      path: 'shared/build.gradle.kts',
      preimage: 'fd66179022462615f054ccf03af25bc8b9399dc6b492d75b022bf2961f0f034e',
      postimage: '00a4789ebba1fea4cfd6e0d3ef298fda7af7a462306ede91081d01c94cd74a92'
    }, {
      path: 'react-native-gradle-plugin/src/main/kotlin/com/facebook/react/ReactPlugin.kt',
      preimage: '0d8511fc8c814b40a910b894986665784ef36a977c139923e92bfc62ef4cb845',
      postimage: '8d47cc51b9759d19a400fc015a455cd137b867c8db69b06f19e7ed4e50d30151'
    }, {
      path: 'react-native-gradle-plugin/src/main/kotlin/com/facebook/react/utils/AgpConfiguratorUtils.kt',
      preimage: 'd25dbfc8d9ee685269e22cddff9f4e86691fe922074dde32e1c5662d9a406309',
      postimage: 'd1e7e31b236484cf06521e64eadff3c85502e822c08351de7caf7a78ed51bea0'
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

function hasCallerIdentity(repair) {
  try {
    const caller = callerRoot(repair);
    const manifest = JSON.parse(fs.readFileSync(path.join(caller, 'package.json'), 'utf8'));
    return manifest.name === repair.caller && manifest.version === repair.callerVersion;
  } catch {
    return false;
  }
}

function selectedRepairs(name) {
  const candidates = repairs[name];
  if (name !== 'rngp') return candidates;
  const selected = candidates.filter(hasCallerIdentity);
  if (selected.length !== 1) fail('unsupported React Native Gradle Plugin version');
  return selected;
}

const name = process.argv[2] || 'cli';
if (!Object.prototype.hasOwnProperty.call(repairs, name)) {
  fail('unsupported repair');
}
for (const repair of selectedRepairs(name)) {
  applyRepair(repair);
}
