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
    $records/                       # the records directory (record_file.records_dir)
      <recordID>.<ext>              # one file per record (top-level)
      <parentRecordID>/             # a record's own directory, next to its record file,
        <subcollection>/            #   holds its subcollections
          $records/
            <recordID>.<ext>
```

### Nesting (parent-scoped records)

A subcollection's data lives **under its parent record's directory**, interleaving
parent-record ids with subcollection names — mirroring Firestore's
document/subcollection model. This is what physically scopes records by their
parent chain, so records with the same leaf collection + id under different parents
never collide.

The parent record's directory is the one the inGitDB definition defines
(ingitdb-go `subcollection-record-validation#req:subcollection-storage-convention`,
the layout `ingitdb validate` reads): it sits **in the parent's records directory**,
next to the parent's record file. The records directory is the parent's
`record_file.records_dir`; when that is omitted it is `$records` for a `{key}` name.

```
spaces/$records/family.yaml                         # record spaces/family
spaces/$records/family/contacts/$records/c1.yaml    # dal key: spaces/family/contacts/c1
spaces/$records/work/contacts/$records/c1.yaml      # dal key: spaces/work/contacts/c1
```

With `records_dir: .` on the parent (the Firestore-mirror layout) the records
directory is the collection directory itself, so the same records are
`spaces/family.yaml` and `spaces/family/contacts/$records/c1.yaml`.

**Mislaid data is an error.** Versions before this one wrote subcollection data at
`<collection>/<parentRecordID>/<subcollection>/` whatever the parent's
`records_dir`. When the parent's records directory is not the collection directory
and files exist at that older place, every read and write of that instance fails
with `ErrMislaidSubCollection`, naming the found path, the file count, the path the
definition gives and the fix (move the files, or set `records_dir: .` on the parent
and move its records), instead of reading it as empty or writing a second copy. The
check runs at every level of the parent chain, so an instance below a mislaid
ancestor, or mislaid below a correctly placed one, fails the same way.

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
