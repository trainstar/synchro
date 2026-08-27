import React, { useEffect, useRef, useState } from 'react';
import {
  StyleSheet,
  Text,
  TextInput,
  TouchableOpacity,
  View,
} from 'react-native';
import {
  encodeConformanceEnvelope,
  type JSONValue,
  MAXIMUM_COMMAND_BYTES,
  MAXIMUM_RESULT_BYTES,
  parseConformanceCommand,
  utf8ByteLength,
} from './types';
import {
  ConformanceCommandError,
  PublicConformanceRunner,
} from './runner';

interface ConformanceHarnessProps {
  serverURL: string;
  authToken: string;
  appVersion: string;
}

type CommandState = 'ready' | 'running' | 'ok' | 'error';

const EMPTY_RESPONSE = encodeConformanceEnvelope({
  schema_version: 1,
  outcome: 'error',
  result: null,
  error_code: 'invalid_command',
});

export function ConformanceHarness({
  serverURL,
  authToken,
  appVersion,
}: ConformanceHarnessProps) {
  const runnerRef = useRef<PublicConformanceRunner | null>(null);
  if (runnerRef.current === null) {
    runnerRef.current = new PublicConformanceRunner({ serverURL, authToken, appVersion });
  }

  const [commandText, setCommandText] = useState('');
  const [state, setState] = useState<CommandState>('ready');
  const [resultText, setResultText] = useState(EMPTY_RESPONSE);

  useEffect(() => {
    const runner = runnerRef.current;
    return () => {
      void runner?.close().catch(() => {
        // The process can terminate while native ownership is already released.
      });
    };
  }, []);

  const execute = async () => {
    setState('running');
    setResultText(EMPTY_RESPONSE);
    try {
      if (utf8ByteLength(commandText) > MAXIMUM_COMMAND_BYTES) {
        throw new ConformanceCommandError('invalid_command');
      }
      const command = parseConformanceCommand(commandText);
      const result = await runnerRef.current!.execute(command);
      const envelope = encodeConformanceEnvelope({
        schema_version: 1,
        outcome: 'passed',
        result: result as unknown as JSONValue,
        error_code: null,
      });
      if (utf8ByteLength(envelope) > MAXIMUM_RESULT_BYTES) {
        throw new ConformanceCommandError('execution_failed');
      }
      setResultText(envelope);
      setState('ok');
    } catch (error) {
      const code = error instanceof ConformanceCommandError
        ? error.code
        : 'invalid_command';
      setResultText(encodeConformanceEnvelope({
        schema_version: 1,
        outcome: 'error',
        result: null,
        error_code: code,
      }));
      setState('error');
    }
  };

  return (
    <View style={styles.container} testID="conformance-harness">
      <Text accessibilityRole="header" style={styles.heading}>
        Native Conformance Command
      </Text>
      <Text style={styles.instructions}>
        Enter one catalog-bound native action for the external test controller.
      </Text>
      <Text style={styles.inputLabel}>Command JSON</Text>
      <TextInput
        accessibilityLabel="Conformance command JSON"
        accessibilityHint="Enter one bounded native conformance command in JSON."
        autoCapitalize="none"
        autoCorrect={false}
        keyboardType="ascii-capable"
        multiline
        maxLength={MAXIMUM_COMMAND_BYTES}
        onChangeText={setCommandText}
        placeholder="Paste one conformance command"
        returnKeyType="done"
        spellCheck={false}
        style={styles.input}
        submitBehavior="blurAndSubmit"
        testID="conformance-command-input"
        value={commandText}
      />
      <TouchableOpacity
        accessibilityLabel="Execute conformance command"
        accessibilityRole="button"
        accessibilityState={{ disabled: state === 'running', busy: state === 'running' }}
        disabled={state === 'running'}
        onPress={() => {
          void execute();
        }}
        style={[styles.button, state === 'running' ? styles.buttonDisabled : undefined]}
        testID="btn-conformance-execute"
      >
        <Text style={styles.buttonText}>Execute Conformance Command</Text>
      </TouchableOpacity>
      <Text
        accessibilityLabel="Conformance command state"
        accessibilityLiveRegion="polite"
        testID="conformance-command-state"
      >
        {state}
      </Text>
      <Text
        accessibilityLabel="Conformance command response"
        accessibilityLiveRegion="polite"
        selectable
        style={styles.result}
        testID="conformance-result"
      >
        {resultText}
      </Text>
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    borderColor: '#4d4d4d',
    borderRadius: 6,
    borderWidth: 1,
    marginTop: 24,
    padding: 12,
  },
  heading: {
    fontSize: 17,
    fontWeight: '600',
  },
  instructions: {
    marginTop: 4,
  },
  inputLabel: {
    fontWeight: '600',
    marginTop: 12,
  },
  input: {
    borderColor: '#4d4d4d',
    borderRadius: 6,
    borderWidth: 1,
    height: 160,
    marginTop: 4,
    padding: 10,
    textAlignVertical: 'top',
  },
  button: {
    alignItems: 'center',
    backgroundColor: '#e0e0e0',
    borderRadius: 6,
    marginTop: 12,
    minHeight: 44,
    padding: 12,
  },
  buttonDisabled: {
    opacity: 0.5,
  },
  buttonText: {
    color: '#1a1a1a',
    fontWeight: '600',
  },
  result: {
    marginTop: 8,
  },
});
