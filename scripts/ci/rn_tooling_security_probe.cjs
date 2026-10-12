'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const crypto = require('node:crypto');
const path = require('node:path');
const { createRequire } = require('node:module');
const { spawnSync } = require('node:child_process');
const { Readable } = require('node:stream');
const { finished } = require('node:stream/promises');

if (process.env.SYNCHRO_TOOLING_ERROR_RECORD) {
  const Module = require('node:module');
  const originalLoad = Module._load;
  const wrapped = new WeakSet();
  const errors = [];
  Module._load = function (...args) {
    const value = Reflect.apply(originalLoad, this, args);
    if (args[0] === 'fast-glob' && !wrapped.has(value)) {
      wrapped.add(value);
      const originalSync = value.sync;
      value.sync = function (...parameters) {
        try {
          return Reflect.apply(originalSync, this, parameters);
        } catch (error) {
          errors.push({ name: error.name, code: error.code });
          throw error;
        }
      };
    }
    return value;
  };
  process.on('exit', () => fs.writeFileSync(process.env.SYNCHRO_TOOLING_ERROR_RECORD, JSON.stringify(errors)));
} else {
  main().catch(error => {
    process.stderr.write(String(error.stack || error) + '\n');
    process.exitCode = 1;
  });
}

async function main() {
  const [mode, root, work] = process.argv.slice(2);
  assert.ok(['cli', 'assembler', 'delivery', 'directories'].includes(mode), 'unknown tooling proof selection');
  const rn = createRequire(path.join(root, 'clients/react-native/package.json'));
  const example = createRequire(path.join(root, 'clients/react-native/example/package.json'));
  const cases = mode === 'cli' ? cliProof(root, work, rn, example)
    : mode === 'delivery' ? deliveryProof(root, work, rn, example)
    : mode === 'directories' ? directoryProof(work, rn, example)
    : await assemblerProof(work, example);
  process.stdout.write(JSON.stringify({ cases }) + '\n');
}

function cliProof(root, work, rn, example) {
  const cliPackage = example.resolve('@react-native-community/cli/package.json');
  const cli = path.join(path.dirname(cliPackage), 'build/bin.js');
  const android = example.resolve('@react-native-community/cli-config-android');
  const project = path.join(work, 'project');
  const fixture = path.join(project, 'node_modules/fixture-native');
  fs.mkdirSync(fixture, { recursive: true });
  fs.writeFileSync(path.join(project, 'package.json'), JSON.stringify({ name: 'tooling-proof', version: '1.0.0', dependencies: { 'fixture-native': '0.0.0' } }));
  fs.writeFileSync(path.join(project, 'react-native.config.js'), `
const android = require(${JSON.stringify(android)});
module.exports = {
  reactNativePath: ${JSON.stringify(path.dirname(rn.resolve('react-native/package.json')))},
  dependencies: {'fixture-native': {root: ${JSON.stringify(fixture)}}},
  platforms: {android: {projectConfig: android.projectConfig, dependencyConfig: android.dependencyConfig}}
};
`);
  const file = (relative, contents) => {
    const target = path.join(fixture, relative);
    fs.mkdirSync(path.dirname(target), { recursive: true });
    fs.writeFileSync(target, contents);
  };
  file('android/build.gradle', "plugins { id 'com.android.library' }\nandroid { namespace 'com.example.fixture' }\n");
  file('android/src/main/AndroidManifest.xml', '<manifest xmlns:android="http://schemas.android.com/apk/res/android" package="com.example.fixture"/>');
  file('android/src/main/java/com/example/fixture/FixturePackage.java', `
package com.example.fixture;
import com.facebook.react.ReactPackage;
import com.facebook.react.bridge.NativeModule;
import com.facebook.react.bridge.ReactApplicationContext;
import com.facebook.react.uimanager.ViewManager;
import java.util.Collections;
import java.util.List;
public class FixturePackage implements ReactPackage {
  public List<NativeModule> createNativeModules(ReactApplicationContext context) { return Collections.emptyList(); }
  public List<ViewManager> createViewManagers(ReactApplicationContext context) { return Collections.emptyList(); }
}
`);
  const component = name => `import codegenNativeComponent from 'react-native/Libraries/Utilities/codegenNativeComponent';\nexport default codegenNativeComponent('${name}');\n`;
  const ordinary = ['JsProbe', 'JsxProbe', 'TsProbe', 'TsxProbe'];
  ['js', 'jsx', 'ts', 'tsx'].forEach((extension, index) => file(`src/Probe.${extension}`, component(ordinary[index])));
  file('src/Duplicate.js', component('JsProbe'));
  file('src/node_modules/ignored/Ignored.js', component('IgnoredProbe'));
  file('src/{a,b}/Literal.js', component('BraceLiteral'));
  file('src/[ab]/Literal.js', component('BracketLiteral'));
  file('src/a/Decoy.js', component('DecoyA'));
  file('src/b/Decoy.js', component('DecoyB'));
  const tests = [
    ['ordinary', 'src', ordinary.concat(['BraceLiteral', 'BracketLiteral', 'DecoyA', 'DecoyB'])],
    ['brace-directory', 'src/{a,b}', ['BraceLiteral']],
    ['bracket-directory', 'src/[ab]', ['BracketLiteral']],
    ['missing', 'missing-source-directory', []],
    ['default', null, ordinary.concat(['BraceLiteral', 'BracketLiteral', 'DecoyA', 'DecoyB'])],
    ['overlength', '{'.repeat(4800) + 'a,b' + '}'.repeat(4800), null],
  ];
  for (const [name, directory, expected] of tests) {
    const codegenConfig = { name: 'FixtureSpec', type: 'components' };
    if (directory !== null) codegenConfig.jsSrcsDir = directory;
    fs.writeFileSync(path.join(fixture, 'package.json'), JSON.stringify({ name: 'fixture-native', version: '0.0.0', codegenConfig }));
    const errorRecord = path.join(work, name + '-errors.json');
    const result = spawnSync(process.execPath, [
      '--max-old-space-size=61', '--max-semi-space-size=1', '--require', __filename, cli, 'config',
    ], {
      cwd: project,
      env: { ...process.env, SYNCHRO_TOOLING_ERROR_RECORD: errorRecord, NO_COLOR: '1' },
      encoding: 'utf8', timeout: 5000, maxBuffer: 1024 * 1024,
    });
    assert.ifError(result.error);
    assert.equal(result.signal, null, name);
    const errors = JSON.parse(fs.readFileSync(errorRecord, 'utf8'));
    if (expected === null) {
      assert.notEqual(result.status, 0, name);
      assert.deepEqual(errors, [{ name: 'SyntaxError' }], name);
    } else {
      assert.equal(result.status, 0, `${name}: ${result.stderr}`);
      assert.deepEqual(errors, [], name);
      const config = JSON.parse(result.stdout);
      const descriptors = config.dependencies['fixture-native'].platforms.android.componentDescriptors;
      assert.deepEqual(descriptors.slice().sort(), expected.map(value => value + 'ComponentDescriptor').sort(), name);
    }
  }
  return tests.length;
}

async function assemblerProof(work, example) {
  const detox = createRequire(example.resolve('detox/package.json'));
  const bunyaminPath = detox.resolve('bunyamin');
  const consumer = createRequire(bunyaminPath);
  const Assembler = consumer('stream-json/Assembler');
  const parser = consumer('stream-json').parser;
  assert.equal(consumer('stream-json/package.json').version, '1.9.1');
  const vectors = [
    '{"__proto__":{"marker":true},"tail":7}',
    '{"__proto__":null,"tail":7}',
    '{"__proto__":"data","tail":7}',
    '{"user":{"__proto__":{"marker":true},"tail":7}}',
    '{"__proto__":null,"__proto__":{"second":true},"tail":7}',
  ];
  const assemble = async (text, reviver) => {
    const assembler = new Assembler(reviver ? { reviver } : undefined);
    const tokens = Readable.from([text]).pipe(parser());
    assembler.connectTo(tokens);
    await finished(tokens);
    return assembler.current;
  };
  let cases = 0;
  for (const text of vectors) {
    for (const reviver of [undefined, (key, value) => value]) {
      const actual = await assemble(text, reviver);
      const expected = JSON.parse(text, reviver);
      assert.deepEqual(actual, expected);
      const object = actual.user || actual;
      assert.equal(Object.getPrototypeOf(object), Object.prototype);
      const descriptor = Object.getOwnPropertyDescriptor(object, '__proto__');
      assert.ok(descriptor);
      assert.equal(descriptor.writable, true);
      assert.equal(descriptor.enumerable, true);
      assert.equal(descriptor.configurable, true);
      cases++;
    }
  }
  for (const replacement of [undefined, { replaced: true }]) {
    const reviver = (key, value) => key === '__proto__' ? replacement : value;
    assert.deepEqual(await assemble(vectors[0], reviver), JSON.parse(vectors[0], reviver));
    cases++;
  }
  let receiver;
  const reviver = function (key, value) {
    receiver = this;
    return value;
  };
  const receiverAssembler = new Assembler({ reviver });
  const receiverTokens = Readable.from([vectors[0]]).pipe(parser());
  receiverAssembler.connectTo(receiverTokens);
  await finished(receiverTokens);
  assert.equal(receiver, receiverAssembler);
  assert.deepEqual(receiverAssembler.current, JSON.parse(vectors[0]));
  cases++;
  const trace = path.join(work, 'trace.json');
  fs.writeFileSync(trace, '[{"name":"reference","ph":"i","ts":10,"pid":7,"tid":5,"args":{"__proto__":{"marker":true}}},{"name":"carrier","ph":"i","ts":20,"pid":7,"__proto__":{"tid":1}}]');
  const records = [];
  for await (const event of require(bunyaminPath).uniteTraceEvents([trace], { mode: 'pid' })) records.push(event);
  assert.equal(records[0].name, 'reference');
  assert.equal(records[0].tid, 0);
  assert.equal(Object.getPrototypeOf(records[0].args), Object.prototype);
  assert.ok(Object.hasOwn(records[0].args, '__proto__'));
  assert.equal(records[0].args.marker, undefined);
  return cases + 1;
}

function deliveryProof(root, work, rn, example) {
  const base = path.join(work, 'installed');
  const modules = path.join(base, 'node_modules');
  const assets = path.join(modules, '@trainstar/synchro-react-native/tooling-patches');
  fs.mkdirSync(modules, { recursive: true });
  fs.cpSync(path.join(root, 'clients/react-native/tooling-patches'), assets, { recursive: true });
  const configName = '@react-native-community/cli-config-android';
  const packages = [
    ['@react-native-community/cli', example],
    ['@react-native-community/cli-platform-android', example],
    [configName, example],
    ['@react-native-community/cli-clean', example],
    ['react-native-builder-bob', rn],
  ];
  const bunyaminEntry = createRequire(example.resolve('detox/package.json')).resolve('bunyamin');
  let bunyamin = path.dirname(bunyaminEntry);
  while (!fs.existsSync(path.join(bunyamin, 'package.json'))) bunyamin = path.dirname(bunyamin);
  fs.cpSync(bunyamin, path.join(modules, 'bunyamin'), { recursive: true });
  packages.push(['stream-json', createRequire(bunyaminEntry)]);
  for (const [name, consumer] of packages) {
    const source = path.dirname(consumer.resolve(name + '/package.json'));
    fs.cpSync(source, path.join(modules, name), { recursive: true });
  }
  const repairs = [
    [configName, 'cli-config-android-20.2.0.patch'],
    ['stream-json', 'stream-json-1.9.1.patch'],
    ['react-native-builder-bob', 'react-native-builder-bob-0.40.18.patch'],
    ['@react-native-community/cli-clean', 'cli-clean-20.2.0.patch'],
  ];
  for (const [name, patch] of repairs) {
    const result = spawnSync('patch', ['--batch', '--reverse', '--fuzz=0', '-p1', '-d', path.join(modules, name)], {
      input: fs.readFileSync(path.join(assets, patch)), encoding: 'utf8', timeout: 5000,
    });
    assert.ifError(result.error);
    assert.equal(result.status, 0, result.stdout + result.stderr);
  }
  const invoke = (app, mode) => spawnSync(process.execPath, [
    path.join(app, 'node_modules/@trainstar/synchro-react-native/tooling-patches/apply.cjs'), mode,
  ], { cwd: app, encoding: 'utf8', timeout: 5000 });
  const target = app => path.join(app, 'node_modules', configName);
  const source = app => path.join(target(app), 'build/config/findComponentDescriptors.js');
  const digest = file => crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex');
  const baseline = digest(source(base));
  let cases = 0;
  for (const mode of ['cli', 'stream-json', 'bob']) {
    const app = path.join(work, 'apply-' + mode);
    fs.cpSync(base, app, { recursive: true });
    const first = invoke(app, mode);
    assert.ifError(first.error);
    assert.equal(first.status, 0, first.stdout + first.stderr);
    assert.match(first.stdout, /applied verified/);
    const second = invoke(app, mode);
    assert.ifError(second.error);
    assert.equal(second.status, 0, second.stdout + second.stderr);
    assert.match(second.stdout, /already has the verified patch/);
    assert.doesNotMatch(second.stdout, /applied verified/);
    if (mode === 'cli') assert.notEqual(digest(source(app)), baseline);
    cases += 2;
  }
  const rejected = [
    ['caller-version', app => {
      const file = path.join(app, 'node_modules/@react-native-community/cli-platform-android/package.json');
      const manifest = JSON.parse(fs.readFileSync(file));
      manifest.version = '20.2.1';
      fs.writeFileSync(file, JSON.stringify(manifest));
    }, /unexpected package version/],
    ['version', app => {
      const file = path.join(target(app), 'package.json');
      const manifest = JSON.parse(fs.readFileSync(file));
      manifest.version = '20.2.1';
      fs.writeFileSync(file, JSON.stringify(manifest));
    }, /unexpected package version/],
    ['identity', app => {
      const file = path.join(target(app), 'package.json');
      const manifest = JSON.parse(fs.readFileSync(file));
      manifest.name = 'different-package';
      fs.writeFileSync(file, JSON.stringify(manifest));
    }, /unexpected package identity/],
    ['hash', app => fs.appendFileSync(source(app), '\n// changed source\n'), /unexpected source hash/],
    ['outside', app => {
      const outside = path.join(work, 'outside');
      fs.cpSync(target(app), outside, { recursive: true });
      fs.rmSync(target(app), { recursive: true });
      fs.symlinkSync(outside, target(app), 'dir');
    }, /outside the caller installation/],
    ['source-link', app => {
      const outside = path.join(work, 'outside-source.js');
      fs.copyFileSync(source(app), outside);
      fs.unlinkSync(source(app));
      fs.symlinkSync(outside, source(app));
    }, /unexpected or missing source target/],
    ['patch-failure', app => {
      const patch = path.join(app, 'node_modules/@trainstar/synchro-react-native/tooling-patches/cli-config-android-20.2.0.patch');
      fs.writeFileSync(patch, 'invalid patch\n');
    }, /patch command failed/],
  ];
  for (const [name, change, message] of rejected) {
    const app = path.join(work, 'reject-' + name);
    fs.cpSync(base, app, { recursive: true });
    change(app);
    const before = digest(source(app));
    const result = invoke(app, 'cli');
    assert.ifError(result.error);
    assert.notEqual(result.status, 0, name);
    assert.match(result.stderr, message, name);
    assert.equal(digest(source(app)), before, name + ' changed the rejected source');
    cases++;
  }
  for (const mode of ['unknown', '__proto__']) {
    const result = invoke(base, mode);
    assert.ifError(result.error);
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /unsupported repair/);
    assert.equal(digest(source(base)), baseline);
    cases++;
  }
  for (const mixed of [false, true]) {
    const app = path.join(work, mixed ? 'mixed-bob' : 'changed-bob');
    fs.cpSync(base, app, { recursive: true });
    const directory = path.join(app, 'node_modules/react-native-builder-bob/lib/targets');
    if (mixed) {
      fs.copyFileSync(path.join(path.dirname(rn.resolve('react-native-builder-bob/package.json')), 'lib/targets/module.js'), path.join(directory, 'module.js'));
    } else {
      fs.appendFileSync(path.join(directory, 'typescript.js'), '\n// changed source\n');
    }
    const before = ['module.js', 'typescript.js'].map(file => digest(path.join(directory, file)));
    const result = invoke(app, 'bob');
    assert.ifError(result.error);
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /unexpected source hash/);
    assert.deepEqual(['module.js', 'typescript.js'].map(file => digest(path.join(directory, file))), before);
    cases++;
  }
  return cases;
}

function directoryProof(work, rn, example) {
  const clean = example.resolve('@react-native-community/cli-clean');
  const bob = path.join(path.dirname(rn.resolve('react-native-builder-bob/package.json')), 'bin/bob');
  const modules = path.join(path.dirname(rn.resolve('typescript/package.json')), '..');
  let cases = 0;
  for (const [name, decoys] of [['cache{a,b}', ['cachea', 'cacheb']], ['cache[ab]', ['cachea', 'cacheb']]]) {
    const temporary = path.join(work, name);
    const preserved = [];
    for (const directory of [name, ...decoys]) {
      for (const cache of ['metro-proof', 'haste-map-proof', 'react-proof']) {
        const marker = path.join(work, directory, cache, 'marker');
        fs.mkdirSync(path.dirname(marker), { recursive: true });
        fs.writeFileSync(marker, 'preserve outside the selected directory');
        if (directory !== name) preserved.push(marker);
      }
    }
    fs.writeFileSync(path.join(temporary, 'unrelated'), 'preserve unrelated data');
    const result = spawnSync(process.execPath, ['-e', `
require(${JSON.stringify(clean)}).commands.clean.func([], {}, {include: 'metro', projectRoot: ${JSON.stringify(work)}})
  .catch(error => { console.error(error); process.exitCode = 1; });
`], { env: { ...process.env, TMPDIR: temporary, TMP: temporary, TEMP: temporary, NO_COLOR: '1' }, encoding: 'utf8', timeout: 5000 });
    assert.ifError(result.error);
    assert.equal(result.status, 0, result.stdout + result.stderr);
    for (const cache of ['metro-proof', 'haste-map-proof', 'react-proof']) assert.equal(fs.existsSync(path.join(temporary, cache)), false);
    assert.equal(fs.readFileSync(path.join(temporary, 'unrelated'), 'utf8'), 'preserve unrelated data');
    for (const marker of preserved) assert.equal(fs.readFileSync(marker, 'utf8'), 'preserve outside the selected directory');
    cases++;
  }
  for (const name of ['project', 'project{a,b}', 'project[ab]']) {
    const project = path.join(work, name);
    fs.mkdirSync(path.join(project, 'src'), { recursive: true });
    fs.symlinkSync(modules, path.join(project, 'node_modules'), 'dir');
    fs.writeFileSync(path.join(project, 'src/index.ts'), 'export const proof: number = 7;\n');
    fs.writeFileSync(path.join(project, 'package.json'), JSON.stringify({
      name: 'literal-directory-proof', version: '1.0.0', source: 'src/index.ts',
      types: 'lib/typescript/index.d.ts',
      'react-native-builder-bob': { source: 'src', output: 'lib', targets: ['module', 'typescript'] },
    }));
    fs.writeFileSync(path.join(project, 'tsconfig.json'), JSON.stringify({
      compilerOptions: {
        declaration: true, emitDeclarationOnly: true, incremental: true,
        tsBuildInfoFile: 'lib/typescript/tsconfig.tsbuildinfo', types: [], skipLibCheck: true,
      }, include: ['src'],
    }));
    const decoys = [];
    for (const output of ['module', 'typescript']) {
      for (const directory of [name, 'projecta', 'projectb']) {
        const marker = path.join(work, directory, 'lib', output, 'old-marker');
        fs.mkdirSync(path.dirname(marker), { recursive: true });
        fs.writeFileSync(marker, 'old build');
        if (directory !== name) decoys.push(marker);
      }
    }
    const result = spawnSync(process.execPath, [bob, 'build'], {
      cwd: project, env: { ...process.env, PATH: path.join(modules, '.bin') + path.delimiter + process.env.PATH, NO_COLOR: '1' },
      encoding: 'utf8', timeout: 25000,
    });
    assert.ifError(result.error);
    assert.equal(result.status, 0, result.stdout + result.stderr);
    assert.match(fs.readFileSync(path.join(project, 'lib/module/index.js'), 'utf8'), /proof/);
    assert.match(fs.readFileSync(path.join(project, 'lib/typescript/index.d.ts'), 'utf8'), /proof/);
    assert.equal(fs.existsSync(path.join(project, 'lib/typescript/tsconfig.tsbuildinfo')), false);
    for (const output of ['module', 'typescript']) assert.equal(fs.existsSync(path.join(project, 'lib', output, 'old-marker')), false);
    for (const marker of decoys) assert.equal(fs.readFileSync(marker, 'utf8'), 'old build');
    cases++;
  }
  return cases;
}
