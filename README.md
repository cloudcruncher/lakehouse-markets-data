# lakehouse-markets-data

Markets & Payments Intelligence: the data engineering team's repo. It builds on the
[open-lakehouse](https://github.com/cloudcruncher/open-lakehouse) platform as a tenant
([ADR 14](https://github.com/cloudcruncher/open-lakehouse/blob/main/docs/adr/0014-platform-and-tenant-teams-in-separate-repos.md)):
the platform owns Kafka, Polaris, Trino, OPA and Dagster; this repo owns its pipelines, its
tables in `markets_bronze` / `markets_silver` / `markets_gold`, and their contracts.

| Source | Lands in | Status |
|---|---|---|
| ECB reference rates (Frankfurter API, daily) | `markets_bronze.fx_rates` | done |
| Coinbase WebSocket trades (stream) | `markets.coinbase.trades` -> `markets_bronze` | next |
| Card authorisations (ShadowTraffic, stream) | `markets.payments.card-auths` -> `markets_bronze` | planned |
| Sanctions lists (OpenSanctions / HMT, daily) | `markets_bronze` | planned |

## Working here

```bash
make lint test        # seconds, no Docker
make run              # against a local platform: in ../open-lakehouse run `make up` first
make contracts        # the platform's contract check, as CI runs it
```

`make run` builds the code-location image (`FROM ghcr.io/cloudcruncher/open-lakehouse-spark:0.2.0`),
joins the platform's networks by name, and runs as this tenant's own Polaris identity: the
container sees only `/run/tenant-secrets/polaris.env`, never the platform's credentials.

## What the platform gives this repo

Onboarded by `tenants/markets-data.yaml` in the platform repo: Kafka topics (`markets.*`),
Polaris namespaces with write access to its own only, a Keycloak group, and a Dagster code
location once `codeLocation.deploy: true`. Changing any of those is a PR there, reviewed by the
platform team. The full interface is in the platform's `tenants/README.md`.

CI stays fast (the target is a release an hour): lint and unit tests without Docker, the
platform's contract check (reusable workflow, pinned to `v0.2.0`), and an image build that
loads the code location.
