# 002: image tag scheme

Stage: S0 · Date: 2026-11-21 · Author: anand24mishra · Drafted by agent: yes

## Choice

Tag production container images using the short Git commit SHA prefix (`sha-${GITHUB_SHA::7}`).

## Rejected alternative

Using the mutable `:latest` tag or maintaining manual Semantic Versioning (`vX.Y.Z`) on every commit.

## Why

A Git SHA tag is immutable and provides an exact 1-to-1 deterministic mapping between the deployed container running on the host and the exact commit that produced it. If an issue occurs, rolling back is trivial and instant: point Docker Compose at the previous commit SHA without needing rebuilds. A mutable `latest` tag obscures what code is actually running and makes rollbacks non-deterministic.

## What would show this was wrong

If external third-party consumers depended on our container image and required human-readable SemVer releases to manage backward-incompatible API changes.
