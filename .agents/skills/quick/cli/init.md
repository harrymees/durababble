# quick init

Initialize a Quick project. Sets up a git repo, downloads Quick skills, and creates starter files.

## Usage

```bash
quick init [site-name]
```

If `site-name` is omitted, prompts for one (defaults to the directory name).

## New project (empty folder)

```bash
mkdir my-site && cd my-site
quick init my-site
```

Creates a git repo with origin at `https://my-site.quick.shopify.io/`, a starter `index.html`, `.gitignore`, Quick skills, and an initial commit. You're ready to build.

If the site name is already taken, offers to choose another name, set up the repo anyway (for existing sites you maintain), or remix it.

When ready to deploy:

```bash
quick deploy . my-site                   # deploy site
git push origin main                     # push source to Quick
```

## Existing repo

```bash
cd my-existing-project
quick init
```

Downloads Quick skills. Does not touch remotes or create files.

## Prerequisites

- `git` installed
- `gcloud` CLI installed and authenticated (`gcloud auth login`)
