# dalgo2ingitdb

`dalgo2ingitdb` is the DALgo adapter for an InGitDB project stored in a local
Git working tree.

## Owner access policies

An owner can enable persisted read and write policies by creating
`.ingitdb/access/manifest.yaml`:

```yaml
enabled: true
database: my-database
policies:
  - readers.yaml
```

Policy paths are relative to `.ingitdb/access` and use the portable
`dtql.org/access/v1` format. The manifest and every referenced policy are
loaded when `NewDatabase` opens the project. That immutable policy snapshot is
used until the database is opened again; editing files does not change an
already-open handle.

Projects without an `.ingitdb/access` directory retain legacy behavior. Once
that directory exists, `manifest.yaml` is required and invalid configuration
fails the open. This first storage slice reads local files only. It does not
yet provide policy generations, atomic policy reloads, or history-based
selection.
