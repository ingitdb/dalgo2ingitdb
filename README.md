# dalgo2ingitdb

`dalgo2ingitdb` is the DALgo adapter for an InGitDB project stored in a local
Git working tree.

## Owner access policies

An owner can enable persisted read and write policies by creating
`.ingitdb/access/manifest.yaml`:

```yaml
enabled: true
database: my-database
realm: example.com
policies:
  - readers.yaml
```

Policy paths are relative to `.ingitdb/access` and use the portable
`dtql.org/access/v1` format. The manifest and every referenced policy are
loaded when `NewDatabase` opens the project. That immutable policy snapshot is
used until the database is opened again; editing files does not change an
already-open handle.

`realm` is optional for compatibility with legacy untyped principals. When it
is set, role, group, and user bindings match typed user principals in that
realm. A service, application, or agent with the same ID does not inherit a
human user binding.

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

Editable Git-backed owners can opt into immutable generations. The active
manifest then names a SHA-256 generation under
`.ingitdb/access/generations/<digest>`. Each generation contains its own
manifest and canonicalized YAML policy files. Publication validates the whole
set, fsyncs and renames the generation, creates a Git commit from a disposable
index, advances `HEAD` with compare-and-swap, and only then replaces the
working active pointer and live compiled snapshot. Startup treats committed
`HEAD` as authoritative and reconstructs a missing or stale working pointer;
unreferenced incomplete generations never become active. Flat policy lists
remain supported for read-only legacy configuration and do not opt into this
publication protocol.

`OwnerPolicyController.Publish` and the matching method on a generation-backed
database require the expected active generation revision. `Reload` validates
and compiles one complete committed generation for atomic installation. The protected coordinator acquires the storage boundary before policy leases
and retains them through evidence, authorization and commit. Publication and
filesystem writes share the adapter writer lock. Mounted reload/publication
also serialize snapshot activation so an older reload cannot undo a revocation.

Query cancellation is cooperative. The adapter checks the context before and
after loading and while converting loaded rows, and `GetMulti` checks between
records. A single filesystem read, YAML decode, formula evaluation in legacy
mode, or in-memory sort runs to completion before cancellation is observed.

Protected query row predicates use DALgo's shared evaluator, including nested
fields, type distinctions, missing-versus-null handling and IN. Restrictions
apply before offset/limit. Synthetic `$id` predicates are unsupported in this
profile because point policy evaluation addresses stored fields; exact-key
access uses the resource path. `$id` remains supported for deterministic ordering
and record identity, and is never injected into a stored policy image.
