# quick auth

Authenticate with Google OAuth and manage credentials. Also sets up git access to Quick repos.

## Usage

```bash
quick auth                          # login (refresh or browser flow)
quick auth --force                  # always do full browser flow
quick auth print-identity-token     # print IAP token to stdout
quick auth revoke                   # remove local credentials
```

## When You Need This

- Before running `quick mcp` for the first time
- If git operations to Quick repos fail (re-run to refresh credentials)
