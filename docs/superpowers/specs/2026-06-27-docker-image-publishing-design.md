# Docker image publishing — design

**Date:** 2026-06-27
**Status:** Approved

## Goal

Publish a container image for kafka-http-bridge so it can be deployed in
production near a Kafka cluster, without forcing operators to build from source.
A `Dockerfile` (multi-stage, `scratch`-based) already exists; this adds the
release automation and the user-facing documentation.

## Usage model (why these choices)

- **Local compose (`compose.yaml`) is dev/test only.** It runs a 3-broker KRaft
  cluster + Kafbat UI, and the bridge is run on the host via `go run`. This is
  not the deployment story and is left unchanged.
- **Real usage is in production, near a Kafka cluster.** Operators either build
  from source or run the published image, pointing `KAFKA_BROKERS` at a broker
  reachable by DNS. There is no host-networking caveat in this path — the
  `host.docker.internal` awkwardness only exists when a container talks to the
  local dev compose stack, which is explicitly not a goal here.

## Scope

In scope:
1. A GitHub Actions workflow that builds and pushes the image to GHCR on version
   tags.
2. A README section documenting the published image and build-from-source for
   production deployment.

Out of scope (deliberate):
- Adding the bridge as a service in `compose.yaml`.
- Pushing images on every push to `main`.
- Multi-arch builds (can be added later if a consumer needs arm64).

## 1. Release workflow — `.github/workflows/release.yml`

- **Trigger:** push of tags matching the glob `'[0-9]*.[0-9]*.[0-9]*'` — semver
  with **no `v` prefix** (e.g. `1.2.3`).
- **Permissions:** job-scoped, minimal:
  - `contents: read`
  - `packages: write`
  Pushes to `ghcr.io/alexsoft/kafka-http-bridge` using the built-in
  `GITHUB_TOKEN` (no PAT).
- **Steps:**
  1. `actions/checkout` with `persist-credentials: false`.
  2. `docker/login-action` → GHCR, authenticating as the workflow actor with
     `GITHUB_TOKEN`.
  3. `docker/metadata-action` → computes tags and labels.
  4. `docker/build-push-action` → builds the existing `Dockerfile` (`prod`
     stage) and pushes.
- **Image tags produced per release:**
  - `1.2.3` — full version (`type=semver,pattern={{version}}`)
  - `1.2` — major.minor (`type=semver,pattern={{major}}.{{minor}}`)
  - `sha-<short>` — commit hash, for traceability (`type=sha`)
  - `latest` — newest release (metadata-action default on semver tags)
- **Hardening:** all third-party actions pinned to commit SHAs with a version
  comment, matching the existing workflows so the repo's zizmor-pedantic check
  keeps passing.

## 2. README — deployment documentation

Add a section covering the two production paths, both pointed at a reachable
cluster (no compose caveat):

```bash
# run the published image
docker run -p 8080:8080 -e KAFKA_BROKERS=kafka.prod.internal:9092 \
  ghcr.io/alexsoft/kafka-http-bridge:1.2.3

# or build from source
go build -o kafka-http-bridge ./cmd/app
```

The existing local-compose Quick Start stays as-is, framed as the dev/test path.

## Testing / verification

- No application code changes, so no new Go tests.
- Workflow correctness is verified by pushing a real version tag and confirming
  the image and its tag set appear in GHCR. (The first tag push is the
  integration test.)
- `gofmt`/`vet`/unit tests are unaffected.

## Risks

- **First-run auth:** GHCR package visibility/permissions must allow the repo's
  `GITHUB_TOKEN` to push. If the package doesn't exist yet, the first push
  creates it as private; making it public (if desired) is a one-time GHCR
  setting.
- **Tag glob:** the glob matches any `N*.N*.N*` tag; a stray non-release tag in
  that shape would trigger a build. Acceptable given the team's tagging
  discipline.
