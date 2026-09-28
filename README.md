# lakehouse-markets-data

Markets & Payments Intelligence: the data engineering team's repo. It builds on the
[open-lakehouse](https://github.com/cloudcruncher/open-lakehouse) platform as a tenant
([ADR 14](https://github.com/cloudcruncher/open-lakehouse/blob/main/docs/adr/0014-platform-and-tenant-teams-in-separate-repos.md)):
the platform owns Kafka, Polaris, Trino, OPA and Dagster; this repo owns its pipelines, its
tables in `markets_bronze` / `markets_silver` / `markets_gold`, and their contracts.

| Source | Lands in | Status |
|---|---|---|
| ECB reference rates (Frankfurter API, daily) | `markets_bronze.fx_rates` | done |
| Coinbase WebSocket trades (stream) | `markets.coinbase.trades` -> `markets_bronze.trades` -> `markets_silver.trades` | done |
| Card authorisations (ShadowTraffic, stream) | `markets.payments.card-auths` -> `markets_bronze.card_auths` -> `markets_silver.card_auths` (EUR), rejects -> `.dlq` | done |
| UK Sanctions List (FCDO via OpenSanctions, daily) | `markets_bronze.sanctions_targets` -> `markets_silver.sanctions_names` | done |
| Gold (hourly): candles, daily card authorisations, sanctions hits | `markets_gold.crypto_ohlcv_1m`, `card_auth_daily`, `sanctions_hits` | done |

## Working here

```bash
make lint test        # seconds, no Docker
make run              # against a local platform: in ../open-lakehouse run `make up` first
make run ASSET=markets_bronze/sanctions_targets,markets_silver/sanctions_names  # the sanctions list
make feed             # the Coinbase producer -> markets.coinbase.trades, against the local platform
make stream           # the trades stream -> markets_bronze.trades, markets_silver.trades (resumes from /state)
make card-stream      # the card-auths stream -> markets_bronze/silver.card_auths, rejects to the DLQ topic
make spark-check      # both streams' Spark transforms on sample records, inside the image (~20 s)
make card-auths-sample  # 5 generated card authorisations, printed (needs the licence, see below)
make contracts        # the platform's contract check, as CI runs it
```

`make run` builds the code-location image (`FROM ghcr.io/cloudcruncher/open-lakehouse-spark:0.3.0`),
joins the platform's networks by name, and runs as this tenant's own Polaris identity: the
container sees only `/run/tenant-secrets/polaris.env`, never the platform's credentials.

The Coinbase producer (`src/markets_data/coinbase.py`) is not a Dagster asset: it runs for as
long as the platform runs it, as a tenant service declared in the platform's tenant file, from
this same image. It needs no API key, keeps no state (Kafka is the durable store), and keys each
trade by product so one product's trades stay in order on one partition. Price and size stay
strings until the stream parses them as decimals.

The trades stream (`jobs/trades_stream.py`, Kappa: Kafka is the source of truth) is a tenant
service too, with its checkpoints on the platform's `/state` volume. Bronze keeps every record
as it arrived; silver keeps each valid trade once (MERGE by product and trade id, since Coinbase
resends the latest trade after a reconnect); records that fail a rule (`trades.REJECT_RULES`)
land in `markets_bronze.trades_rejects` with the reason. A lost checkpoint doesn't duplicate
bronze: the stream resumes after the offsets the table already holds.

## What the platform gives this repo

Onboarded by `tenants/markets-data.yaml` in the platform repo: Kafka topics (`markets.*`),
Polaris namespaces with write access to its own only, a Keycloak group, and a Dagster code
location once `codeLocation.deploy: true`. Changing any of those is a PR there, reviewed by the
platform team. The full interface is in the platform's `tenants/README.md`.

CI stays fast (the target is a release an hour): lint and unit tests without Docker, the
platform's contract check (reusable workflow, pinned to `v0.3.0`), and an image build that
loads the code location.

## Card authorisations (ShadowTraffic)

`generators/card-auths/` is ShadowTraffic with this team's config: ~1 authorisation a second (laptop scale; see Volumes)
from 5,000 cards (keyed by card token) at 16 merchants, in seven currencies that all have an ECB
rate, plus ~1% malformed records for the stream's dead-letter topic. It ships as
`lakehouse-markets-data:card-auths-<version>` and runs as a tenant service.

The licence is this team's, never in git: the platform tenant file declares the slot, and the
team stores it once from the platform repo with
`make tenant-secret TENANT=markets-data NAME=shadowtraffic FILE=<licence.env>`. Local runs here
and the platform's service read the same file. Without it (the platform's CI) the generator
idles and says why; when the trial expires, store the renewed file the same way.

## The card-authorisation stream

`jobs/card_auths_stream.py` is the second Kappa stream, run as the tenant service
`card-auths-stream` with its own `/state` (so it fails, restarts and replays apart from trades).
It shares the trades stream's shape (`streaming.py`): bronze appends every record; silver MERGEs
each valid authorisation once by `auth_id`, converted to euro at the latest ECB fixing on or
before its day (`markets_bronze.fx_rates`, at most 7 days old). A record that fails a rule
(`card_auths.REJECT_RULES`) lands in `markets_bronze.card_auths_rejects` with the reason and on
`markets.payments.card-auths.dlq` as JSON (reason, topic, partition, offset, payload). The table
is exactly once; the DLQ topic is at least once, so its consumers key on partition and offset.
A known currency with no recent rate is rejected as "no ECB rate in the week before": the FX job
is behind, and a replay after it catches up converts those records.

## Sanctions list and merchant screening

`jobs/sanctions.py` (Dagster, daily at 06:15 London) lands the UK Sanctions List (FCDO, as
OpenSanctions republishes it; CC BY-NC) in `markets_bronze.sanctions_targets`, one row per target
ever listed: MERGEd by id, with `delisted_at` once a target leaves the list. A list with fewer
than 1,000 targets is treated as a broken download, not as mass delisting. Birth dates,
addresses, phones and emails are never landed: screening doesn't need them.

`markets_silver.sanctions_names` holds today's normalised names and aliases of listed
organisations (people and vessels aren't merchants). `sanctions.normalise()` runs on both sides
of a screen: lower case, no accents or punctuation, no legal forms (`PJSC "Aeroflot"` ->
`aeroflot`), at least 4 characters. Matching is exact on those names; transliterations need a
fuzzy matcher, which is not built yet. The card generator has one listed merchant (Aeroflot,
~0.6% of authorisations) so screening has something real to find.

Memory: both streams run with a 512 MB driver heap and `MALLOC_ARENA_MAX=2` (`submit.py`).
Measured on 28 Sep 2026, that took a stream from 1195 to about 660 MiB in its 1280 MB service.

## Gold data products

`jobs/gold.py` (Dagster multi-asset `gold`, hourly at :20) builds three tables readable by every
colleague: `crypto_ohlcv_1m` (one-minute candles with VWAP), `card_auth_daily` (per day, merchant
country, currency and channel, in euro, with approval rate) and `sanctions_hits` (merchants whose
normalised name is on today's list). Candles and daily totals rebuild the last two days and
replace those day partitions; hits are replaced whole, so a delisted match drops out. Five asset
checks (`gold.CHECKS`) run after each build and show in Dagster. No card token reaches gold.

## Streams in one application

The platform runs both streams as one service (`jobs/streams.py`): four queries in one Spark
driver, which saves ~1 GB against two drivers. Each query keeps its own checkpoint, so either
stream can still be replayed alone, and a query without a checkpoint resumes after the offsets
its tables already hold. `trades_stream.py` and `card_auths_stream.py` still run alone
(`make stream`, `make card-stream`). The card stream caches the ECB rates and reloads them hourly.

## Volumes (laptop scale)

The platform runs on one 16 GB laptop, so the defaults prove every path at small volume rather
than load. Each is one setting on a bigger machine:

| What | Laptop default | Where to scale it |
|---|---|---|
| Coinbase books | BTC-EUR, ETH-EUR (~1 trade/s) | `PRODUCTS` env (e.g. add BTC-USD, ETH-USD, SOL-USD: ~7/s) |
| Card authorisations | ~1/s, ~1% malformed | `throttleMs` in `generators/card-auths/card-auths.json` |
| Stream commits | every 2 minutes per query | `TRIGGER` env |
| Catch-up batch | at most 10,000 records | `MAX_OFFSETS_PER_TRIGGER` env |
| Driver heap | 512m per stream, 768m for both / batch jobs | `DRIVER_MEMORY` in `submit.py`, or `SPARK_DRIVER_MEMORY` |
