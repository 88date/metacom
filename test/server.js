'use strict';

const timers = require('node:timers/promises');
const { WebSocket } = require('ws');
const { test } = require('node:test');
const assert = require('node:assert');
const { Server } = require('../lib/server.js');

const { emitWarning } = process;
process.emitWarning = (warning, type, ...args) => {
  if (type === 'ExperimentalWarning') return;
  emitWarning(warning, type, ...args);
  return;
};

class ProcedureMock {
  constructor({ access, raw = false, ...options }) {
    this.options = options;
    this.access = access;
    this.exports = { raw };
    this.method = options.handler;
  }

  // eslint-disable-next-line class-methods-use-this
  async enter() {}
  // eslint-disable-next-line class-methods-use-this
  leave() {}
  invoke(context, args) {
    return this.options.handler(args, context);
  }
}

test('Server / calls', async (t) => {
  const sessionToken = 'valid-session-token';
  const session = {
    userId: 'user-1',
    role: 'USER',
    language: 'ru',
    appVersion: '1.0.0',
  };
  const closeClient = {
    access: 'public',
    transports: ['centrifugo'],
    handler: async (_args, { client }) => {
      client.close();
      client.close();
      return 'ignored after close';
    },
  };
  const api = {
    test: {
      close: closeClient,
      hello: {
        access: 'public',
        transports: ['http', 'ws'],
        handler: async ({ name }) => {
          await timers.setTimeout(10);
          return `Hello, ${name}`;
        },
      },
      centrifugoOnly: {
        access: 'public',
        transports: ['centrifugo'],
        handler: async ({ name }) => `Hello from Centrifugo, ${name}`,
      },
    },
    channel: {
      subscribe: closeClient,
      publish: closeClient,
    },
  };
  let hookCalls = 0;
  let rawBody = null;
  const rawRouter = new ProcedureMock({
    raw: true,
    handler: async (req, res) => {
      if (req.method === 'OPTIONS') {
        res.writeHead(204, { 'Tus-Version': '1.0.0' });
        res.end();
        return;
      }

      const chunks = [];
      for await (const chunk of req) chunks.push(chunk);
      rawBody = Buffer.concat(chunks).toString();
      res.writeHead(204, {
        'Tus-Resumable': '1.0.0',
        'Upload-Offset': Buffer.byteLength(rawBody),
      });
      res.end();
    },
  });
  const noop = () => {};
  const options = {
    host: 'localhost',
    port: 8003,
    protocol: 'http',
    centrifugo: { secret: 'centrifugo-secret' },
    timeouts: { bind: 100 },
    queue: { concurrency: 100, size: 100, timeout: 5_000 },
  };
  const application = {
    console: { log: noop, info: noop, warn: noop, error: noop, debug: noop },
    static: { constructor: { name: 'Static' } },
    auth: {
      saveSession: async () => {},
      readSession: async (token) =>
        token === sessionToken ? { ...session } : null,
    },
    getMethod: (unit, _version, method, transport) => {
      const definition = api[unit]?.[method];
      if (!definition) return null;
      if (!definition.transports.includes(transport)) return null;
      return new ProcedureMock(definition);
    },
    getHook: (unit) => {
      hookCalls++;
      return unit === 'files' ? { router: rawRouter } : null;
    },
  };
  const connectCentrifugo = async (data) => {
    const response = await fetch(
      `http://${options.host}:${options.port}/api/centrifugo/connect`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Secret: options.centrifugo.secret,
        },
        body: JSON.stringify({ data }),
      },
    );
    return response.json();
  };
  const callCentrifugo = async (method, data) => {
    const response = await fetch(
      `http://${options.host}:${options.port}/api/centrifugo/rpc`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Secret: options.centrifugo.secret,
        },
        body: JSON.stringify({ method, data }),
      },
    );
    return response.json();
  };

  let server;

  t.beforeEach(async () => {
    server = new Server(application, options);
    await server.listen();
  });

  t.afterEach(async () => {
    await server.close();
  });

  await t.test('handles HTTP RPC', async () => {
    const initialHookCalls = hookCalls;
    const id = 1;
    const args = { name: 'Max' };
    const packet = { type: 'call', id, method: 'test/hello', args };
    const response = await fetch(`http://${options.host}:${options.port}/api`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(packet),
    }).then((res) => res.json());

    assert.strictEqual(response.id, id);
    assert.strictEqual(response.type, 'callback');
    assert.strictEqual(response.result, `Hello, ${args.name}`);
    assert.strictEqual(hookCalls, initialHookCalls);
  });

  await t.test('rejects Centrifugo-only methods over HTTP', async () => {
    const packet = {
      type: 'call',
      id: 1,
      method: 'test/centrifugoOnly',
      args: { name: 'Max' },
    };
    const response = await fetch(`http://${options.host}:${options.port}/api`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(packet),
    });
    const result = await response.json();

    assert.strictEqual(response.status, 404);
    assert.strictEqual(result.error.code, 404);
  });

  await t.test(
    'keeps default OPTIONS handling for regular routes',
    async () => {
      const response = await fetch(
        `http://${options.host}:${options.port}/api/example`,
        { method: 'OPTIONS' },
      );

      assert.strictEqual(response.status, 200);
      assert.strictEqual(response.headers.get('tus-version'), null);
      assert.strictEqual(
        response.headers.get('access-control-allow-methods'),
        'POST, GET, OPTIONS',
      );
    },
  );

  await t.test('forwards OPTIONS to raw routers', async () => {
    const response = await fetch(
      `http://${options.host}:${options.port}/api/files`,
      { method: 'OPTIONS' },
    );

    assert.strictEqual(response.status, 204);
    assert.strictEqual(response.headers.get('tus-version'), '1.0.0');
  });

  await t.test('forwards the unread body stream to raw routers', async () => {
    const body = 'raw upload payload';
    const response = await fetch(
      `http://${options.host}:${options.port}/api/files/upload-id`,
      {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/offset+octet-stream' },
        body,
      },
    );

    assert.strictEqual(response.status, 204);
    assert.strictEqual(response.headers.get('tus-resumable'), '1.0.0');
    assert.strictEqual(
      response.headers.get('upload-offset'),
      Buffer.byteLength(body).toString(),
    );
    assert.strictEqual(rawBody, body);
  });

  await t.test('WS RPC handles', async () => {
    const id = 1;
    const args = { name: 'Max' };
    const packet = { type: 'call', id, method: 'test/hello', args };
    const socket = new WebSocket(`ws://${options.host}:${options.port}`);
    await new Promise((res) => socket.on('open', res));
    socket.send(JSON.stringify(packet));
    const resPacket = await new Promise((res) => socket.on('message', res));
    const response = JSON.parse(resPacket);
    assert.strictEqual(response.id, id);
    assert.strictEqual(response.type, 'callback');
    assert.strictEqual(response.result, `Hello, ${args.name}`);
  });

  await t.test('rejects Centrifugo-only methods over WS', async () => {
    const packet = {
      type: 'call',
      id: 1,
      method: 'test/centrifugoOnly',
      args: { name: 'Max' },
    };
    const socket = new WebSocket(`ws://${options.host}:${options.port}`);
    await new Promise((resolve) => socket.on('open', resolve));
    socket.send(JSON.stringify(packet));
    const data = await new Promise((resolve) => socket.on('message', resolve));
    const response = JSON.parse(data);
    socket.close();

    assert.strictEqual(response.error.code, 404);
  });

  await t.test('allows Centrifugo-only methods over Centrifugo', async () => {
    const response = await callCentrifugo('test/centrifugoOnly', {
      name: 'Max',
    });

    assert.deepStrictEqual(response, {
      result: {
        data: { ok: true, result: 'Hello from Centrifugo, Max' },
      },
    });
  });

  await t.test('rejects unknown Centrifugo sessions', async () => {
    const response = await connectCentrifugo({
      token: 'unknown-session-token',
      userId: 'attacker',
      role: 'ADMIN',
    });

    assert.deepStrictEqual(response, {
      disconnect: { code: 4501, reason: 'unauthorized' },
    });
  });

  for (const route of ['rpc', 'subscribe', 'publish']) {
    await t.test(`disconnects Centrifugo clients during ${route}`, async () => {
      const response = await fetch(
        `http://${options.host}:${options.port}/api/centrifugo/${route}`,
        {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            Secret: options.centrifugo.secret,
          },
          body: JSON.stringify({ method: 'test/close', channel: 'test' }),
        },
      );

      assert.strictEqual(response.status, 200);
      assert.strictEqual(
        response.headers.get('content-type'),
        'application/json',
      );
      assert.deepStrictEqual(await response.json(), {
        disconnect: { code: 4500, reason: 'closed' },
      });
    });
  }

  await t.test('accepts only allowed Centrifugo session state', async () => {
    const response = await connectCentrifugo({
      token: sessionToken,
      userId: 'attacker',
      role: 'ADMIN',
      language: 'en',
      appVersion: '1.2.3',
      permissions: ['admin'],
    });

    assert.deepStrictEqual(response, {
      result: {
        user: session.userId,
        meta: {
          ...session,
          token: sessionToken,
          language: 'en',
          appVersion: '1.2.3',
        },
      },
    });
  });

  await t.test('keeps missing Centrifugo session state', async () => {
    const response = await connectCentrifugo({ token: sessionToken });

    assert.deepStrictEqual(response, {
      result: {
        user: session.userId,
        meta: { ...session, token: sessionToken },
      },
    });
  });
});
