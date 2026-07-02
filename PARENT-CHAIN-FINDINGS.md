# dalgo2ingitdb — parent-chain / per-space scoping findings

**Date:** 2026-07-02 · **Status:** investigation complete, fix deferred to an owner decision on on-disk format.

## Summary

`dalgo2ingitdb` (and the ingitdb data model beneath it) does **not** physically
scope nested/subcollection data by the parent record's ID. Two dalgo keys that
share a leaf collection + record ID but differ only in their parent chain —
e.g. `spaces/family/contacts/c1` vs `spaces/work/contacts/c1` — resolve to the
**same file on disk** and clobber each other.

This is the concrete gap behind the OVDB / self-host "data not per-space-scoped
on disk" note. It matters because per-space scoping is a **removal criterion**
for dropping the "Experimental" label on the OVDB storage choice
(see `sneat-co/backstage/docs/roadmaps/space-storage-strategy.md`).

## Root cause (code-cited)

The gap is in **two layers**, and the deeper one is the ingitdb data model, not
just the dalgo key resolver:

1. **dalgo2ingitdb resolver — drops the parent chain.**
   `tx_readonly.go:177 resolveCollection()`:
   ```go
   collectionID := key.Collection()             // leaf only, e.g. "contacts"
   colDef, ok := r.def.Collections[collectionID] // flat top-level lookup
   recordKey := fmt.Sprintf("%v", key.ID)        // leaf id only
   ```
   `key.Parent()` is never consulted. The same flat `key.Collection()` pattern is
   duplicated in `tx_readwrite.go` (Set ~55, get-before-write ~114, Delete ~180,
   Update ~262/290) and `Exists` (~103).

2. **ingitdb data model — subcollection data dir is not parameterized by parent
   record id.** `ingitdb-go/ingitdb/validator/def_validator.go:127-142`: a
   collection's `DirPath` is derived from the **schema layout only**
   (`dataBase = colDir`; `DirPath = dataBase [+ DataDir]`). Subcollections are
   discovered from the schema tree (`loadSubCollectionsShared(schemaDir, dataBase,…)`,
   `def_validator.go:164`) — there is **no** point at which a parent *record* id
   (e.g. `family`) is injected into a subcollection's data path. So a subcollection
   is stored as a single data directory per *definition*, unlike Firestore where
   `spaces/{id}/contacts` is one collection **per parent doc**.

`dal.Key` fully exposes what we'd need — `Parent() *Key`, `Collection()`,
`CollectionPath()` (→ `spaces/contacts`), `String()` (→ `spaces/family/contacts/c1`),
`ID` — so the information to scope correctly is available at the adapter; the
missing piece is where to put the parent-record id on disk.

## The design fork (needs an owner call on on-disk format)

Because there are **no users yet** (backwards-compat is explicitly waived) the
format is free to choose, but the two options differ a lot in code blast-radius:

### Option A — nest subcollection data under the parent record dir (Firestore-mirroring) ✅ recommended
On-disk: `spaces/family/contacts/c1.json` (data physically nested; human-readable;
matches the mental model and the "own your data / browse your repo" pitch).
- **Where:** core `ingitdb-go` — `DirPath` computation + record IO must accept a
  parent-record-id path segment for each level of nesting; the reader/query layer
  must resolve a subcollection instance relative to a specific parent record.
- **Cost/risk:** touches the working schema engine (readers, queries, record IO,
  validators). Highest correctness risk; needs its own test matrix. This is the
  "right" long-term layout for the data-ownership product.

### Option B — encode the parent chain into the leaf path inside the flat collection dir (localized)
On-disk: e.g. `contacts/spaces~family/c1.json` (parent chain escaped into a
subdir or key prefix within the existing flat collection data dir).
- **Where:** `dalgo2ingitdb` only — the shared resolver computes a
  parent-scoped path; **queries** (`ExecuteQuery*`) must filter to the parent
  scope too, or listing a subcollection leaks sibling scopes.
- **Cost/risk:** no `ingitdb-go` change, but the on-disk layout is less
  schema-native and the query path must be made scope-aware or we regress
  listing (a "do not lose functionality" trap). Medium risk, contained.

**Recommendation:** Option A — it's the correct owned-data layout and the whole
point of OVDB is a readable repo. But it's a core-engine change, so it should be
done deliberately (own branch, full test matrix), not slipped in. Until it lands,
the OVDB storage choice stays **Experimental** (already recorded).

## Also flagged (do not edit from here)
`openvaultdb-go/internal/store/store.go` has the **same class of flaw** at
`collectionRelPath(nsID, collection)` — it maps `(namespace, collection)` → path
with no parent-record scoping. Whatever format Option A/B picks should be applied
there too.

## What was NOT done and why
No code fix was applied: the correct fix (Option A) is a core ingitdb-go
data-model change with high correctness blast-radius, and the format choice is an
owner decision. A localized Option-B fix risks silently regressing subcollection
queries. Deferred pending the format decision. The bug itself is objectively
proven by the code paths cited above.
