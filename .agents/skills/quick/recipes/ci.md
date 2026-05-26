# GitHub Actions (CI/CD)

**This is how teams collaborate on Quick sites.** Put your site in a GitHub repo, add a workflow, and anyone on the team can contribute via PRs. Merges to main auto-deploy. PRs get preview environments that clean up automatically.

Two composite actions handle everything: `deploy-to-quick` and `delete-from-quick`. Authentication uses Workload Identity Federation — no secrets or service account keys needed.

## Which action to use

**Pick based on the GitHub org your repo lives in.** GitHub Actions can't resolve private composite actions across orgs using the default `GITHUB_TOKEN`, so the right reference depends on where your repo lives.

| Your repo's org | Use |
|---|---|
| `shopify-playground/*` | [`shopify-playground/deploy-to-quick@main`](https://github.com/shopify-playground/deploy-to-quick) + [`shopify-playground/delete-from-quick@main`](https://github.com/shopify-playground/delete-from-quick) |
| `Shopify/*` | `Shopify/skai-train/.github/actions/deploy-to-quick@main` + `Shopify/skai-train/.github/actions/delete-from-quick@main` |

The playground actions are standalone internal-visibility repos maintained alongside skai-train — same code, same auth, same inputs. They exist so playground repos don't have to work around the cross-org private-action restriction.

> ⚠️ If you reference `Shopify/skai-train/...@main` from a `shopify-playground` repo, GitHub Actions will fail with `Unable to resolve action Shopify/skai-train, repository not found`. Use the `shopify-playground/*` refs in that case.

## Deploy to Quick

```yaml
# shopify-playground/* repos
- uses: shopify-playground/deploy-to-quick@main
  with:
    dir: dist
    site_name: my-site

# Shopify/* repos
- uses: Shopify/skai-train/.github/actions/deploy-to-quick@main
  with:
    dir: dist
    site_name: my-site
```

### Inputs

| Input | Required | Description |
|---|---|---|
| `dir` | Yes | Directory to deploy (must contain `index.html`) |
| `site_name` | Yes | Subdomain name (lowercase alphanumeric + hyphens, max 63 chars) |
| `ignore_patterns` | No | Additional patterns to exclude (multiline or comma-separated) |

The action automatically excludes `node_modules`, lockfiles, dotfiles, `.md` files, and `LICENSE`.

### What it does

1. Validates the site name and directory
2. Authenticates via Workload Identity
3. Packages the directory as a tar.gz (excluding ignored files)
4. Sends to a Cloud Function that uploads to GCS
5. Site is live at `https://<site_name>.quick.shopify.io`

## Delete from Quick

```yaml
# shopify-playground/* repos
- uses: shopify-playground/delete-from-quick@main
  with:
    site_name: my-site-pr-123

# Shopify/* repos
- uses: Shopify/skai-train/.github/actions/delete-from-quick@main
  with:
    site_name: my-site-pr-123
```

### Inputs

| Input | Required | Description |
|---|---|---|
| `site_name` | Yes | Subdomain name of the site to delete |

Returns success even if the site doesn't exist (404 is non-fatal).

## Example: Deploy on Push (shopify-playground repo)

```yaml
name: Deploy to Quick
on:
  push:
    branches: [main]

permissions:
  contents: read
  id-token: write  # Required for Workload Identity

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Build
        run: pnpm ci && pnpm run build

      - uses: shopify-playground/deploy-to-quick@main
        with:
          dir: dist
          site_name: my-app
```

For a `Shopify/*` repo, swap the `uses:` line to `Shopify/skai-train/.github/actions/deploy-to-quick@main`.

## Example: PR Previews (shopify-playground repo)

Deploy a preview on PR open/update, clean up on PR close:

```yaml
name: PR Preview
on:
  pull_request:
    types: [opened, synchronize, reopened, closed]

permissions:
  contents: read
  id-token: write

jobs:
  preview:
    runs-on: ubuntu-latest
    if: github.event.action != 'closed'
    steps:
      - uses: actions/checkout@v4
      - run: pnpm ci && pnpm run build
      - uses: shopify-playground/deploy-to-quick@main
        with:
          dir: dist
          site_name: my-app-pr-${{ github.event.number }}

  cleanup:
    runs-on: ubuntu-latest
    if: github.event.action == 'closed'
    steps:
      - uses: shopify-playground/delete-from-quick@main
        with:
          site_name: my-app-pr-${{ github.event.number }}
```

For a `Shopify/*` repo, swap both `uses:` lines to the corresponding `Shopify/skai-train/.github/actions/...` refs.

## Authentication

Uses [Workload Identity Federation](https://cloud.google.com/iam/docs/workload-identity-federation) — no secrets or service account keys. Your workflow needs:

```yaml
permissions:
  contents: read
  id-token: write  # Required
```

The actions authenticate against `quick-gha@shopify-skai-train.iam.gserviceaccount.com` via the `github-actions-pool` identity pool.
