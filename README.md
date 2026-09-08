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

Computed columns are not returned while owner policies are enabled. Formula
evaluation currently has no way to prove that the policy also authorizes every
stored or cross-collection dependency, so protected reads expose stored fields
only. Foreign-key metadata does not dereference parent records during reads;
computed foreign-key values are treated like other computed values and remain
hidden. Legacy projects continue to evaluate computed columns.

Applications that enforce policies outside this adapter must pass
`WithStoredOnlyReads()` to `NewDatabase`. This selects the same safe
materialization without enabling or replacing the owner manifest. It is needed
when an outer policy wrapper may hide inputs to a computed field, because that
wrapper otherwise receives the computed value after evaluation. Standalone
callers that omit the option and have no owner manifest retain legacy computed
column behavior.

Query cancellation is cooperative. The adapter checks the context before and
after loading and while converting loaded rows, and `GetMulti` checks between
records. A single filesystem read, YAML decode, formula evaluation in legacy
mode, or in-memory sort runs to completion before cancellation is observed.
