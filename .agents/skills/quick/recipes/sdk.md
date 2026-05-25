# Quick Node.js SDK

Use the Quick SDK to interact with any Quick site from Node.js — scripts, automation, agent tools. Provides authenticated access to databases, files, real-time events, sockets, data warehouse, and more.

## Prerequisites

- `quick auth login` (stores credentials in `~/.config/quick/`)
- Node.js 18+

## Install

```bash
pnpm install @shopify/quick
```

## Setup

```js
import { createClient } from '@shopify/quick/sdk';
const { db, fs, socket } = createClient('<my-site>');
```

`createClient` accepts a site name or full URL:
- `'my-site'` resolves to `https://my-site.quick.shopify.io`

Authentication is automatic — cached IAP credentials from `quick auth login` are loaded and refreshed transparently.

## Modules

### db — Database

Full CRUD with fluent query builder and real-time SSE subscriptions.

```js
const { db } = createClient('<my-site>');
```

#### Collections

```js
const collections = await db.getCollections();
const posts = db.collection('posts');
```

#### Read

```js
const all = await posts.find();
const item = await posts.findById('abc-123');

const recent = await posts
  .where({ status: 'published' })
  .orderBy('created_at', 'desc')
  .limit(10)
  .find();

const titles = await posts.select(['title', 'created_at']).find();
const page = await posts.limit(10).offset(20).find();

const tagged = await db.collection('items')
  .arrayContains({ tags: 'priority' })
  .find();
```

#### Write

```js
const doc = await posts.create({ title: 'Hello', body: 'World' });
const docs = await posts.create([{ title: 'A' }, { title: 'B' }]);

await posts.update('abc-123', { title: 'Updated' });
await posts.update([
  { id: 'abc-123', order: 1 },
  { id: 'def-456', order: 2 }
]);
await posts.where({ archived: true }).update({ archived: false });

await posts.delete('abc-123');
await posts.where({ archived: true }).delete();
```

#### Stats

```js
const stats = await posts.getStats();
```

#### Real-time subscriptions (SSE)

Subscribe to collection changes. Works in both browser (native EventSource) and Node.js (polyfilled with authenticated fetch streaming).

```js
const unsubscribe = posts.subscribe({
  onCreate: (doc) => console.log('created', doc),
  onUpdate: (doc) => console.log('updated', doc),
  onDelete: (id) => console.log('deleted', id),
  onConnect: (info) => console.log('connected', info),
  onError: (err) => console.error(err)
});

unsubscribe();
```

Use this to build daemons that react to database changes — trigger notifications, sync data, run pipelines.

### socket — Real-time Rooms

Join named rooms for presence, shared state, and ephemeral messaging between browsers, scripts, and agents. Uses Socket.IO under the hood.

IMPORTANT: Do not add real-time/WebSocket features unless explicitly asked.

```js
const { socket } = createClient('<my-site>');
const room = socket.room('my-room');
await room.join();
```

#### Presence

```js
console.log(room.users); // Map<socketId, user>
console.log(room.user);  // Current user

room.on('user:join', (user) => console.log(`${user.name} joined`));
room.on('user:leave', (user) => console.log(`${user.name} left`));
```

#### Shared state

```js
room.updateUserState({ status: 'active', cursor: { x: 10, y: 20 } });

room.on('user:state', (prevState, nextState, user) => {
  console.log(`${user.name} state:`, nextState);
});
```

#### Custom events

```js
room.emit('chat', { text: 'hello from node!' });

room.on('chat', (data, sender) => {
  console.log(`${sender.name}: ${data.text}`);
});
```

#### Cleanup

```js
room.leave();
```

Rooms auto-rejoin on reconnect. A Node.js script and a browser tab on the same site joining the same room name will see each other.

### fs — File System

Read, write, and manage files on a Quick site's storage bucket.

```js
const { fs } = createClient('<my-site>');

const data = await fs.read('data/config.json');

await fs.write('data/output.json', { results: [1, 2, 3] });
await fs.write('logs/run.txt', 'completed at ' + new Date().toISOString());

const info = await fs.getInfo('data/config.json');
const { url } = await fs.getSignedUrl('data/large-file.csv', { expires: 3600 });
await fs.delete('data/old-file.json');
```

### dw — Data Warehouse (BigQuery)

Run BigQuery queries through Quick's proxy. Requires BigQuery OAuth scope.

```js
const { dw } = createClient('<my-site>');

const { results } = await dw.querySync('SELECT * FROM dataset.table LIMIT 10');

const job = await dw.query('SELECT * FROM big_table');
const result = await job.wait();

const result = await dw.queryAndWait('SELECT count(*) as n FROM dataset.table');
```

### id — User Identity

Returns info about the authenticated user.

```js
const { id } = createClient('<my-site>');

const user = await id.waitForUser();
console.log(user.email, user.fullName);
```

### slack — Slack Messaging

Send messages to Slack channels (requires Slack integration on the site).

```js
const { slack } = createClient('<my-site>');

await slack.sendMessage('#my-channel', 'Hello from a script!');
await slack.sendNotification('#alerts', 'Deploy Complete', 'v1.2.3 is live');
await slack.sendAlert('#ops', 'Disk usage at 90%', 'warning');
```

### func — Cloud Function Proxy

Call GCP Cloud Functions / Cloud Run through Quick's authenticated proxy.

```js
const { func } = createClient('<my-site>');

const resp = await func.get('https://us-central1-project.cloudfunctions.net/myFunc');
const data = await resp.json();
```

### http — HTTP Proxy

Make external HTTP requests through Quick's proxy (useful for APIs that need server-side auth).

```js
const { http } = createClient('<my-site>');

const resp = await http.get('https://api.example.com/data');
const data = await resp.json();
```

## Architecture

```
createClient(siteOrUrl, { getToken? })
  |
  +-- Resolves URL:  'my-site' -> https://my-site.quick.shopify.io
  +-- Loads IAP auth: ~/.config/quick/ credentials, auto-refresh
  +-- Wraps fetch:   injects Authorization: Bearer <token>
  |
  +-- Returns: { db, dw, fs, id, func, http, slack, socket }
                |                                      |
                |                                      +-- Socket.IO rooms
                |                                          (extraHeaders auth)
                |
                +-- Each extends Base(siteOrUrl, { basePath, fetch })
                      +-- this.baseUrl = resolveBaseUrl(siteOrUrl) + basePath
                      +-- this.fetch = authenticated fetch
```
