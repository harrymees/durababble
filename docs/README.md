---
title: "Durababble Docs"
weight: 1
---

This folder contains the Zeitung documentation source for Durababble. Run Zeitung commands from the repository root so `docs/content/` resolves correctly.

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
npm run format:markdown   # Format Markdown docs and repository README files
npm run check:markdown    # Check Markdown formatting; also runs through rake lint
```
