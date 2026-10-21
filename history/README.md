# history

The record of how this repository's history was extracted from the monorepo,
[The Test Cabinet](https://github.com/TheClockwyrks/TheTestCabinet), where the
arena crate lived until the extraction. Nothing builds or tests against these
files; they are kept so the provenance outlives the scratch clone the
extraction ran in.

## The extraction

The history below the scaffold commit is the monorepo's `issue/repos-16` at
`ec5bb0ef9498c9df837b592206b016eb92115334` (its `nightly`; no other crate of
the monorepo depended on this one, so nothing had to be carried out of it
first), filtered to the paths in [`paths.txt`](paths.txt) by git filter-repo
2.47.0:

```sh
git clone -q --no-local --single-branch --branch issue/repos-16 --no-tags \
  <the-test-cabinet> x
cd x
uvx --from git-filter-repo==2.47.0 git-filter-repo --force \
  --paths-from-file ../paths.txt --prune-empty always --preserve-commit-hashes
```

Of the 7,066 commits reachable from that commit, 47 touched a path in the list
and were kept (15 of them merges); the rest were pruned. No tag was carried.
The tip tree of `crates/arena/` hashes the same as the monorepo's at that
commit:

```text
crates/arena   2efb1ca5cf1eec13a2bb4d834358f51ff1caf6c3
```

The paths are kept where they were in the monorepo, so the filter needed no
rename.

## The files

- [`paths.txt`](paths.txt) is the list handed to `--paths-from-file`
  (sha256 `4c46b6ab0d43e047044c13fad7fc5c67030c401292b590850b2307b7582930cf`):
  the crate's directory, `crates/arena/`, and every path a tracked file under
  it had before a rename or copy, collected by
  `git log --follow --name-status -M -C` over each of the 17 files, so a
  file's history reaches back past the commits that moved it into place.
- [`commit-map`](commit-map) is filter-repo's map from each monorepo commit
  (left) to the commit it became here (right), all zeros for a pruned one.
  `--preserve-commit-hashes` left the commit ids quoted in the messages as they
  were, so an id in a message below the scaffold commit names a monorepo
  commit, and this map translates it.

## The live-path review

`paths.txt` names seven paths outside the moved set, each the path a file here
had before it was moved or copied into place:

- `crates/worker/src/error.rs` and `crates/artifacts/src/error.rs`: the error
  types `src/error.rs` began as a copy of.
- `crates/artifacts/Cargo.toml`: the artifacts service's manifest, which the
  crate's own manifest began as a copy of.
- `crates/auth-service/src/main.rs`: the former auth service's entrypoint,
  which `src/main.rs` began as a copy of.
- `crates/backend/src/access.rs`, `access.test.rs` and `access.keys.test.rs`:
  the backend's Cloudflare Access verification, which `src/access.rs` and its
  tests began as a copy of.

The `crates/worker`, `crates/artifacts` and `crates/auth-service` paths are not
live in the monorepo's tree at the extraction commit (the artifacts crate had
moved to `microservice-artifacts`). The three `crates/backend` paths are: the
backend keeps its own Access verification. The filtered tip therefore carried
those three files, and the scaffold commit deletes them, so no file of the
backend is part of this repository's tree.
