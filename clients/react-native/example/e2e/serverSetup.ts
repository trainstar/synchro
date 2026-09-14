import { randomUUID } from 'node:crypto';

const SYNCHRO_TEST_URL = process.env.SYNCHRO_TEST_URL ?? 'http://127.0.0.1:8091';
const USER1_JWT =
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJhMTExMTExMS0xMTExLTExMTEtMTExMS0xMTExMTExMTExMTEiLCJleHAiOjQxMDI0NDQ4MDB9.ZPjufmc-mgkQC6rc6GVNzH9V3jhqQZMl2AuF0Cleuz8';
const USER2_JWT =
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJiMjIyMjIyMi0yMjIyLTIyMjItMjIyMi0yMjIyMjIyMjIyMjIiLCJleHAiOjQxMDI0NDQ4MDB9.md1BWZARNDofCHihSjDmFY6Wr2L1MBf9r-BDc5zrhFE';

type JSONMap = Record<string, any>;

async function postJSON(path: string, token: string, body: JSONMap): Promise<JSONMap> {
  const response = await fetch(`${SYNCHRO_TEST_URL}${path}`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${token}`,
      'X-App-Version': '1.0.0',
    },
    body: JSON.stringify(body),
  });
  const payload = (await response.json()) as JSONMap;
  if (!response.ok) {
    throw new Error(`${path} returned HTTP ${response.status}: ${JSON.stringify(payload)}`);
  }
  return payload;
}

async function connect(token: string, clientID: string): Promise<JSONMap> {
  return postJSON('/sync/connect', token, {
    client_id: clientID,
    platform: 'detox',
    app_version: '1.0.0',
    protocol_version: 3,
    schema: { version: 0, hash: '' },
    scope_set_version: 0,
    known_scopes: {},
  });
}

async function rebuildScopes(
  token: string,
  clientID: string,
  connected: JSONMap
): Promise<JSONMap[]> {
  const records: JSONMap[] = [];
  const schema = {
    version: connected.schema.version,
    hash: connected.schema.hash,
  };
  const scopes = Array.isArray(connected.scopes?.add) ? connected.scopes.add : [];

  for (const scope of scopes) {
    let cursor = scope.cursor ?? null;
    const rebuildID = randomUUID();
    let hasMore = true;
    while (hasMore) {
      const response = await postJSON('/sync/rebuild', token, {
        client_id: clientID,
        client_generation: connected.client_generation,
        schema,
        scope: scope.id,
        rebuild_id: rebuildID,
        cursor,
        limit: 100,
      });
      if (Array.isArray(response.records)) {
        records.push(...response.records);
      }
      hasMore = response.has_more === true;
      cursor = response.cursor ?? null;
    }
  }
  return records;
}

function customerFields(connected: JSONMap): {
  tableID: string;
  idFieldID: string;
  nameFieldID: string;
} {
  const table = connected.schema_definition?.tables?.find(
    (candidate: JSONMap) => candidate.name === 'customers'
  );
  const idField = table?.fields?.find((field: JSONMap) => field.name === 'id');
  const nameField = table?.fields?.find((field: JSONMap) => field.name === 'name');
  if (!table?.table_id || !idField?.field_id || !nameField?.field_id) {
    throw new Error('server setup schema is missing the customers table fields');
  }
  return {
    tableID: table.table_id,
    idFieldID: idField.field_id,
    nameFieldID: nameField.field_id,
  };
}

async function findCustomer(
  token: string,
  recordID: string,
  clientPrefix: string
): Promise<{
  clientID: string;
  connected: JSONMap;
  fields: ReturnType<typeof customerFields>;
  record: JSONMap;
} | null> {
  const clientID = `${clientPrefix}-${randomUUID()}`;
  const connected = await connect(token, clientID);
  const fields = customerFields(connected);
  const records = await rebuildScopes(token, clientID, connected);
  const record = records.find(
    (candidate) =>
      candidate.table === fields.tableID && candidate.pk?.[fields.idFieldID] === recordID
  );
  return record ? { clientID, connected, fields, record } : null;
}

export async function prepareConflict(recordID: string): Promise<void> {
  let found: Awaited<ReturnType<typeof findCustomer>> = null;
  for (let attempt = 0; attempt < 5 && !found; attempt += 1) {
    found = await findCustomer(USER1_JWT, recordID, 'detox-conflict-setup');
    if (!found) {
      await new Promise((resolve) => setTimeout(resolve, 750));
    }
  }
  if (!found?.record?.server_version) {
    throw new Error(`server setup could not find customer ${recordID}`);
  }

  const schema = {
    version: found.connected.schema.version,
    hash: found.connected.schema.hash,
  };
  const response = await postJSON('/sync/push', USER1_JWT, {
    client_id: found.clientID,
    client_generation: found.connected.client_generation,
    batch_id: randomUUID(),
    schema,
    mutations: [
      {
        mutation_id: randomUUID(),
        table: found.fields.tableID,
        op: 'update',
        pk: { [found.fields.idFieldID]: recordID },
        authored_schema: schema,
        base_version: found.record.server_version,
        columns: { [found.fields.nameFieldID]: 'server-version' },
        client_version: '2030-01-01T00:00:00.000000Z',
      },
    ],
  });
  if (response.accepted?.length !== 1 || response.rejected?.length !== 0) {
    throw new Error(`server conflict write was not accepted: ${JSON.stringify(response)}`);
  }
}

export async function assertUserIsolation(recordID: string): Promise<void> {
  const found = await findCustomer(USER2_JWT, recordID, 'detox-isolation-setup');
  if (found) {
    throw new Error(`user 2 can see user 1 customer ${recordID}`);
  }
}
