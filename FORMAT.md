# inGitDB on-disk format (v1)

This is the **cross-language contract** between the Go writer
(`github.com/ingitdb/dalgo2ingitdb`) and the TypeScript reader
(`@ingitdb/client-github` / `@ingitdb/client-fs`). Both sides validate against the
same golden fixtures (`testdata/format-fixtures/` here, mirrored in `ingitdb-ts`)
so a change on either side that breaks the other fails CI, not a user's repo.

Keeping this readable in a plain git repo *is* the product ("own your data"): a
person browsing the repo on GitHub should recognise their data.

## Layout

```
<root>/
  .ingitdb/
    root-collections.yaml           # maps top-level collection id -> path
  <collection>/
    .collection/
      definition.yaml               # schema for <collection>
      subcollections/
        <subcollection>/
          definition.yaml           # schema for a subcollection
    $records/
      <recordID>.<ext>              # one file per record (top-level)
    <parentRecordID>/               # a record's own directory holds its subcollections
      <subcollection>/
        $records/
          <recordID>.<ext>
```

### Nesting (parent-scoped records)

A subcollection's data lives **under its parent record's directory**, interleaving
parent-record ids with subcollection names — mirroring Firestore's
document/subcollection model. This is what physically scopes records by their
parent chain, so records with the same leaf collection + id under different parents
never collide:

```
spaces/family/contacts/$records/c1.yaml     # dal key: spaces/family/contacts/c1
spaces/work/contacts/$records/c1.yaml       # dal key: spaces/work/contacts/c1
```

Top-level collections are unchanged: `spaces/$records/family.yaml` etc.

## Files

- **`.ingitdb/root-collections.yaml`** — `id: path` lines registering top-level
  collections, e.g. `spaces: spaces`.
- **`.collection/definition.yaml`** — the collection schema:
  ```yaml
  record_file:
    name: '{key}.yaml'      # {key} is replaced by the record id
    format: yaml            # yaml | json | md | csv | jsonl | ingr
    type: map[string]any    # SingleRecord: one file per record
  columns:
    name:
      type: string
      required: true
  columns_order:
    - name
  ```
- **Record file** (`$records/{key}.yaml` for `SingleRecord`+`yaml`) — the record's
  fields, e.g. `name: Alice\n`. `{key}` in `record_file.name` is replaced by the
  record id; the `$records/` base path applies when the name contains `{key}`.

## Golden fixtures

`testdata/format-fixtures/` is a canonical repo tree exercising a top-level
collection (`spaces`) and a nested subcollection (`contacts`) under two different
parent spaces. The Go writer contract test asserts the driver produces byte-identical
record files; the TS reader contract test asserts `@ingitdb/client-github` parses the
same tree. The two committed copies (here and in `ingitdb-ts`) must stay identical —
if you change one, change both (and this spec).
