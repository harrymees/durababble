---
name: quick
description: Build and deploy sites on Quick, Shopify's internal static hosting platform. Use when building Quick sites, using Quick APIs (database, AI, file storage, WebSocket, Slack, BigQuery), deploying with the Quick CLI, or working with quick.shopify.io.
---

# Quick

[Quick](https://quick.shopify.io) is Shopify's internal platform for hosting static sites with serverless APIs. Sites are behind Google IAP — only Shopify employees can access them. Each site lives at `<subdomain>.quick.shopify.io`.

## Key Principles

- Quick sites are **frontend only** — static HTML, CSS, JS. No backend.
- Every site must have an `index.html` (or `200.html` for SPA routing).
- Functionality comes from Quick's APIs, not custom servers.
- Include the client library: `<script src="/client/quick.js"></script>`
- Do not add real-time/WebSocket features unless explicitly asked.

## Client Library

Include via script tag. All APIs are on the `quick` global:

```html
<script src="/client/quick.js"></script>
```

### Database (`quick.db`) — [full docs](client/db.md)

Note: Each object in a collection has a maximum size of 1MB.

```javascript
const posts = quick.db.collection("posts");
const doc = await posts.create({ title: "Hello" });
const all = await posts.find();
const one = await posts.findById("id");
await posts.update("id", { title: "Updated" });
await posts.delete("id");

// Query
const results = await posts
  .where({ status: "published" })
  .orderBy("created_at", "desc")
  .limit(10)
  .find();

// Real-time
const unsub = posts.subscribe({
  onCreate: (doc) => {},
  onUpdate: (doc) => {},
  onDelete: (id) => {},
});
```

### AI (`quick.ai`) — [full docs](client/ai.md)

The `/api/ai` endpoint is an OpenAI-compatible proxy. Use the OpenAI SDK directly:

```javascript
import OpenAI from "https://cdn.jsdelivr.net/npm/openai/+esm";
const client = new OpenAI({ baseURL: `/api/ai`, apiKey: "not-needed", dangerouslyAllowBrowser: true });

const stream = await client.chat.completions.create({
  model: "gpt-5.2",
  messages: [{ role: "user", content: "Hello" }],
  stream: true,
});
for await (const chunk of stream) {
  const content = chunk.choices[0]?.delta?.content;
  if (content) console.log(content);
}
```

### File Storage (`quick.fs`) — [full docs](client/fs.md)

```javascript
const result = await quick.fs.uploadFile(file, {
  onProgress: ({ percentage }) => console.log(`${percentage}%`),
});
// => { url, fullUrl, size, mimeType }
```

### WebSocket (`quick.socket`) — [full docs](client/socket.md)

```javascript
const room = quick.socket.room("lobby");
room.on("user:join", (user) => console.log(user.name, "joined"));
room.on("user:state", (prev, next, user) => {});
await room.join();
room.updateUserState({ cursor: { x: 100, y: 200 } });
room.emit("ping", { t: Date.now() });
```

### User Identity (`quick.id`) — [full docs](client/id.md)

```javascript
const user = await quick.id.waitForUser();
// => { email, fullName, slackHandle, slackImageUrl, title, github, team, ... }
quick.id.email; // direct access
```

### Other APIs

- **Site Management** (`quick.site`) — [full docs](client/site.md): Create, get, delete sites programmatically
- **Data Warehouse** (`quick.dw`) — [full docs](client/dw.md): Query BigQuery (requires OAuth via `quick.auth.requestScopes`)
- **Slack** (`quick.slack`) — [full docs](client/slack.md): Send messages, alerts, tables to Slack
- **HTTP Proxy** (`quick.http`) — [full docs](client/http.md): Proxy requests to external APIs (bypasses CORS)
- **Cloud Functions** (`quick.func`) — [full docs](client/func.md): Call GCP Cloud Functions/Cloud Run with IAP auth
- **Auth** (`quick.auth`) — [full docs](client/auth.md): OAuth for Google, GitHub, Slack (popup-based flows)

## Reading a live Quick site

Fastest way to look at a Quick site. If someone shares a URL or asks about a live page, start here.

```bash
curl -H "Authorization: Bearer $(quick auth print-identity-token)" https://<site>.quick.shopify.io
```

Quick sites are behind IAP, so the auth header is required. This gets you the rendered page — enough to answer questions, understand what a site does, or decide if you need to go deeper with source code or MCP.

## CLI

Install: Auto-managed by tec (`tec-up.sh`). Manual: `pnpm i -g @shopify/quick`

| Command | Description | [Docs](cli/) |
|---|---|---|
| `quick init [site-name]` | Initialize project with git repo and Quick skills | [init.md](cli/init.md) |
| `quick deploy <dir> <site-name>` | Deploy a directory to Quick | [deploy.md](cli/deploy.md) |
| `quick remix <site> [dir] [--copy\|--clone]` | Get source or fork a Quick site | [remix.md](cli/remix.md) |
| `quick serve [dir] [sitename]` | Local dev server with full API access | [serve.md](cli/serve.md) |
| `quick auth` | Authenticate with Google OAuth | [auth.md](cli/auth.md) |
| `quick mcp [site]` | Start MCP server for AI integration | [mcp.md](cli/mcp.md) |
| `quick delete <sitename>` | Delete a deployed site | [delete.md](cli/delete.md) |

### Deployment

```bash
quick deploy . my-site                # deploy current directory
quick deploy dist my-app              # deploy build output
quick deploy . my-site --watch        # watch mode — redeploy on changes
quick serve                           # local dev at http://<dir>.quick.localhost:1337
```

`quick deploy <dir> <site-name>` deploys a directory to Quick. Both arguments are required. Every site should include index.html or 200.html.

Default deployment method: When working in a Quick git repo (i.e. this project was created with quick init or has a git origin of <site-name>.quick.shopify.io), always use git push origin main:deploy — do not use quick deploy unless there is a packaged build output directory (e.g. dist/). When a dist/ or similar build output exists, use quick deploy <dist> <site-name> instead.

### Version Control

Quick sites can be backed by git repos. `quick init` creates a repo with origin at `<site>.quick.shopify.io`. Push source with `git push origin main`. Deploy with `git push origin main:deploy`. Remix someone else's site with `quick remix <site-name>`.

**Commit your work routinely.** Make small, meaningful commits as you go — don't wait until everything is done.

For the full git workflow, see [git.md](recipes/git.md).

### Team Collaboration

For team projects on GitHub, use [CI/CD actions](recipes/ci.md) to auto-deploy on merge. PRs get preview environments that clean up automatically.

## Skills (Extended Context)

The `skills` CLI is a separate tool for discovering and installing agent skills — modular context packages that give AIs domain-specific expertise. Skills are **not Quick-specific**. The registry covers topics across Shopify and beyond. But many skills exist that go deeper on Quick features like widgets, dashboards, and integrations.

This skill covers Quick platform fundamentals. Skills are for **Shopify-internal tools, workflows, and Quick ecosystem features** that someone has packaged as reusable agent context — things like specific widgets, internal integrations, or Shopify-specific patterns. For general web technologies, libraries, or anything you can look up in public docs, just use normal research — don't reach for skills.

When a task involves a Quick widget, a Shopify-internal tool, or a Quick-specific integration pattern you're not covered on here, ask the user: "There might be a skill for this — want me to search for one?"

**Always confirm with the user before searching or installing.** If you think a skill might help, tell the user what you'd search for and why. If the search finds something relevant, show them what you found and ask before installing.

```bash
skills search "<terms>"      # find skills by keyword
skills info <name>           # show what a skill does before installing
skills get <name>            # install (only after confirming with the user)
```

### Examples of when to search for a skill

| The task involves... | Try searching | Known skill |
|---|---|---|
| Comments or annotations on a site | `skills search "quickcomments"` | `quickcomments` |
| Voice or video calling | `skills search "call webrtc quick"` | `call` |
| Building a dashboard (BigQuery, charts) | `skills search "quick dashboard"` | `quick-dashboard-guidance` |
| Product search UIs or Shopify Catalog API integration | `skills search "quick-catalog"` | `quick-catalog` |
| A Quick widget or plugin you haven't used before | `skills search "<widget name>"` | varies |
| A Shopify-specific workflow (catalog, Polaris, etc.) | `skills search "<topic>"` | varies |

These are just starting points. The registry grows over time and covers non-Quick topics too — if you're approaching unfamiliar territory, a quick `skills search` costs nothing and may save significant effort.

## Recipes

Extended guides for specific patterns and integrations. Read as needed:

- [Widgets](recipes/widgets.md) — reusable UI components, embeddable bundles
- [200.html](recipes/200-html.md) — SPA routing with catch-all fallback
- [Static Data](recipes/static-data.md) — directory.json, users.json, usage data
- [HTTP MCP](recipes/http-mcp.md) — browser agents + MCP tool access
- [Slack API](recipes/slack-api.md) — Slack bots, OAuth, interactive messages
- [GitHub API](recipes/github-api.md) — GitHub OAuth, repo/PR tools
- [Google Workspace](recipes/gworkspace.md) — Sheets, Drive, Calendar
- [Image Generation](recipes/image-generation.md) — fal.ai, DALL-E, visual content
- [Git](recipes/git.md) — version control, pushing source, remixing sites
- [Vite](recipes/vite.md) — Vite dev server, build tooling, framework integration
- [CI/CD](recipes/ci.md) — GitHub Actions deploy-to-quick / delete-from-quick
- [Node.js SDK](recipes/sdk.md) — programmatic access to Quick sites from Node.js scripts and automation
