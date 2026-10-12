'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
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
  assert.ok(['cli', 'assembler'].includes(mode), 'unknown tooling proof selection');
  const rn = createRequire(path.join(root, 'clients/react-native/package.json'));
  const example = createRequire(path.join(root, 'clients/react-native/example/package.json'));
  const cases = mode === 'cli' ? cliProof(root, work, rn, example) : await assemblerProof(work, example);
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
