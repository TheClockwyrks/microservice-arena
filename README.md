# microservice-arena

The arena: adversarial matches and tournaments, run as CPU-bound wasm off the
backend.

This repository is one component of
[The Test Cabinet](https://github.com/TheClockwyrks/TheTestCabinet). What it is
and how it fits with the others is stated by the documentation site, which
lives in the superrepo at `apps/docs/src/content/docs/`. Its Repositories page,
`apps/docs/src/content/docs/development/repositories.md`, draws the edges
between the repositories.

## The service

`crates/arena` is `tcab-arena`, the arena: it runs adversarial matches and
tournaments on demand, executing each controller as CPU-bound wasm through the
match runner and the Foray host of the engines repository, off the backend.
The console reaches it through the edge for a quick match or a tournament, and
it reads every controller input from the backend and persists finished
tournaments and their replays back to it, over HTTP through the backend client
of the contracts repository. It holds no state of its own, mounts no volume,
and binds `0.0.0.0:8791` by default.

What the service must do is stated by the superrepo's
`apps/docs/src/content/docs/components/arena/overview.md`. Its configuration
is entirely environment variables, documented in `crates/arena/src/config.rs`;
none is required, so with a backend on its loopback default it runs as it is:

```sh
cargo run --bin tcab-arena
```

The service reads `.env.arena` from the working directory itself, and a
variable already exported takes precedence over the file.

The manifests that deploy it (its `Deployment`, `Service` and
`ServiceAccount`) and the deploy sets pinning its image live in the
superrepo, under `deployments/k8s/`. A deployment pins this repository's image
at the commit the superrepo's submodule pointer names.

## Getting started

Open the superrepo in its devcontainer, which carries every toolchain the
repositories need, and work from this repository's root.
A build here inherits the superrepo's Cargo configuration, which redirects
every dependency on another repository to the sibling checkout, so nothing is
fetched from a mirror.

One command runs every gate:

```sh
make gate
```

Each gate is a file under `ci/gates/` whose stem is the gate's id.
`uv run --quiet --project ci gate list` names them all, and
`azure-pipelines.yml` runs each one as a step of its own on every push, inside
the CI images the superrepo builds.

## The image

The `Dockerfile` at the root is the repository's image, built from the root
with `.dockerignore` as its allowlist. On every gated push to `master` and
`staging` the pipeline builds it natively for amd64 and arm64
(`scripts/ci/image.sh`) and fuses the two into the multi-arch
`testcabinet.azurecr.io/microservice-arena:<commit>` a deployment pins
(`scripts/ci/manifest.sh`).
The `build-context` gate holds every `COPY` to the allowlist and the Rust pin
to `rust-toolchain.toml`. The kit wrote the `Dockerfile` once, and it is the
repository's to grow.
