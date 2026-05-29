---
title: "Durababble Docs"
weight: 1
---

This folder contains the Zeitung documentation source for Durababble. Run Zeitung commands from the repository root so `docs/content/` resolves correctly.

> **Note:** Zeitung is a thin wrapper around [Hugo](https://gohugo.io). That is why the build invokes the `hugo` binary directly (in the `docs:build` Rake task and in CI) and the site configuration lives in `hugo.toml`. Everywhere else, "Zeitung" is the name we use for the docs toolchain.

## Local Preview

```shell
devx zeitung
```

Zeitung turns Markdown in `docs/content/` into a documentation website. Build output is written to `docs/build/`.

## Content

The published site content lives in `docs/content/`. Existing design and review notes still live directly under `docs/` so current repository links keep working while the published docs grow.

## Useful Commands

```shell
devx zeitung              # Preview locally on port 1313
devx zeitung --port 8080  # Preview locally on another port
devx zeitung build        # Build static output into docs/build
bundle exec rake docs:query_perf # Generate MySQL query performance reports into docs/static/query-perf
bundle exec rake docs:build      # Generate query reports, then build static output into docs/build
npm run format:markdown   # Format Markdown docs and repository README files
npm run check:markdown    # Check Markdown formatting; also runs through rake lint
```
