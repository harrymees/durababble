# Getting source code for a Quick site

## Just need the files? Use --copy

```bash
quick remix <site> --copy
quick remix <site> ./some/path --copy   # custom destination
```

Downloads the deployed files into a local directory. No git setup, no fork, no site registration. Safe for research and inspection.

## Site has Quick git?

If the site was deployed via `git push`, you can also clone it directly:

```bash
git clone https://<site>.quick.shopify.io <dir>
```

This gives you the repo as-is (not a fork). Use this when you just want the source with history and don't need a new site name.

## Want to fork to a new site?

Forking creates a new site identity — a new name on `*.quick.shopify.io` with its own git remote.

Non-interactive (if the user gives you a name):

```bash
quick remix <site> --clone <fork-name>              # dir defaults to ./fork-name/
quick remix <site> ./some/path --clone <fork-name>   # custom dir
```

Errors if the name is taken or clone isn't available. Interactive fallback — tell the user to run it themselves:

```bash
quick remix <site>
```

## Decision guide

| Goal | Command |
|---|---|
| Grab files to look at | `quick remix <site> --copy` |
| Clone repo with history | `git clone https://<site>.quick.shopify.io` |
| Fork to a new site (user gives name) | `quick remix <site> --clone <fork-name>` |
| Fork to a new site (interactive) | Tell user to run `quick remix <site>` |
