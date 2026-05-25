# quick deploy

Deploy a directory to Quick.

## Usage

```bash
quick deploy <dir> <site-name>           # deploy
quick deploy <dir> <site-name> -w        # deploy and watch for changes
quick deploy <dir> <site-name> -f        # skip confirmation prompts
```

Both `dir` and `site-name` are required.

## Example

```bash
quick deploy . my-site
quick deploy dist my-app --watch
```

## Behavior

1. Validates site name (lowercase alphanumeric + hyphens, max 63 chars)
2. Validates directory contains `index.html` or `200.html`
3. Warns if deploying more than 1,000 files (probably the wrong directory)
4. If site exists, prompts for overwrite confirmation
   - Your own site: Y/n
   - Someone else's site: must type exact site name
5. Deploys files to Quick
   - Excludes: dotfiles, `node_modules`, lockfiles, `LICENSE`, `AGENTS.md`, `CLAUDE.md`

## Prerequisites

- `gcloud` CLI installed and authenticated
- Directory must contain `index.html` or `200.html`

## Notes

- For sites with a build step (e.g. Vite), deploy the output directory: `quick deploy dist my-site`
- Use `--watch` for rapid iteration — auto-redeploys on file changes.
