# Git & Version Control

Quick sites can be backed by git repos hosted at `<site>.quick.shopify.io`. This gives you version history, the ability to roll back changes, and a record of what you've done — without needing GitHub or any external service.

## Setup

Git access is set up automatically by `quick auth` and `quick init`.

## Start a new site

```bash
mkdir my-site && cd my-site
quick init my-site
```

This creates a git repo with origin pointing to Quick, a starter `index.html`, and your first commit. You're ready to build.

## Deploy

Every site should have an `index.html` (or `200.html` for SPA routing).

```bash
quick deploy . my-site                   # deploy current directory
quick deploy dist my-site                # deploy build output
quick deploy . my-site --watch           # redeploy on file changes
```

Both `dir` and `site-name` are required. For sites with a build step (e.g. Vite → `dist/`), deploy the output directory.

## Push source

```bash
git push origin main
```

Push your source to Quick git so it's backed up and available to clone or remix. Pushing source is separate from deploying — it stores your code on Quick's git server for backup, collaboration, and remixing. The git server also handles branch deploys:

- Push to `main` → stores source only (does not deploy)
- Push to `deploy` → deploys to production (`my-site.quick.shopify.io`)
- Push to `deploy--foo` → deploys to staging (`my-site--foo.quick.shopify.io`)

If your repo contains a `dist/`, `public/`, `build/`, or `site/` directory, the server will look for `index.html` there first.

To remove a staging site:

```bash
git push origin :deploy--foo             # deletes the ref and removes the staging site
```

## Remix a site

```bash
quick remix cool-dashboard
```

Start from someone else's deployed site. Copy the files, clone the source, or follow a link to GitHub. Remix asks for a new site name so you don't overwrite the original.

If someone tries to `git clone` a site that was deployed without git, the server will suggest using `quick remix` instead.

## GitHub repos

GitHub source is not synced to Quick git — they are separate systems. If your source lives on GitHub, the cleanest setup is [CI/CD](ci.md) to auto-deploy on merge. You can still deploy locally with `quick deploy dist my-site`, but for team repos CI is strongly preferred.

For solo projects where you don't need GitHub, Quick git is simpler — everything in one place.

## Good practices

- **Commit often.** Small, meaningful commits as you work — not one big commit at the end. This gives you checkpoints to roll back to.
- **Push source.** Keep your remote in sync so it's backed up and available from anywhere.
- **Deploy from a clean tree.** `quick deploy` warns about uncommitted changes for a reason.
