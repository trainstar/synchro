export interface PackagedSmokeConfig {
  schema_version: 1;
  cell_id: string;
  platform: string;
  server_url: string;
  token: string;
  client_id: string;
  phase: 'initial' | 'resume';
  initial_sql: readonly string[];
  durable_sql: readonly string[];
  observe_sql: string;
  result_url: string;
  result_token: string;
}

declare global {
  var __SYNCHRO_PACKAGED_SMOKE_CONFIG__: PackagedSmokeConfig | undefined;
}

if (globalThis.__SYNCHRO_PACKAGED_SMOKE_CONFIG__ === undefined) {
  throw new Error('Packaged smoke runtime configuration is required');
}

export const packagedSmokeConfig = globalThis.__SYNCHRO_PACKAGED_SMOKE_CONFIG__;
