# OneTrust → Glean connector

A small Python connector that pulls **assessments** from the OneTrust API and
pushes them into Glean as searchable documents via the Glean Indexing (push)
API.

## What it does

1. Authenticates to OneTrust with OAuth2 client credentials.
2. Pages through every assessment (`/api/assessment/v2/assessments`).
3. For each assessment, fetches the creator and risk breakdown from the export
   endpoint and writes everything to a local `AssessmentList.csv`.
4. Creates/updates a Glean custom datasource with the right schema (risk counts,
   residual risk score, assessment status as custom properties).
5. Indexes each CSV row into Glean as a document, one per assessment.

```
OneTrust API ──auth+paginate──▶ AssessmentList.csv ──index──▶ Glean
```

### Files

| File | Purpose |
| --- | --- |
| `onetrust.py` | Entry point. Fetches assessments and triggers indexing. |
| `indexer.py` | Glean datasource config + document indexing. |
| `gleanConstants.py` | Loads config from env vars and CLI flags. |
| `gleantools.py` | CSV read/write helpers. |
| `_.env-example` | Template for your `.env`. |
| `init.sh` / `install.sh` / `run.sh` | Environment setup and run scripts. |

## Prerequisites

- Python 3 (`init.sh` uses the system `/usr/bin/python3`).
- A Glean **Indexing API token** with the Indexing scope.
- OneTrust **API credentials** (client id + secret) for your tenant.

## Setup

```bash
cd onetrust

# 1. Create and activate a local virtualenv
./init.sh
source ./__onetrust__/bin/activate

# 2. Install dependencies (Glean client from the hosted zip + requirements.txt)
./install.sh

# 3. Configure your environment
cp _.env-example .env
# then edit .env and fill in the blanks
```

See [`_.env-example`](./_.env-example) for the full list of variables and what
each one does. The connector exits immediately if any required variable is
missing.

## Running

```bash
./run.sh
```

`run.sh` activates the virtualenv, loads `.env`, and runs `onetrust.py`. It will
configure the datasource, crawl OneTrust into `AssessmentList.csv`, then index
the rows into Glean.

### Dry run

Set `DEBUG=true` in `.env` (or pass `-d true`) to build and print every request
**without** calling the Glean API. This is the safest way to validate config and
inspect payloads before writing to your index.

> Heads up: debug/verbose output prints the OneTrust secret and Glean token to
> stdout. Don't run it in shared/CI logs.

### CLI flags

`gleanConstants.py` accepts a few overrides on top of the `.env` values, e.g.:

```bash
python -u onetrust.py -d true        # debug / dry run
python -u onetrust.py -v             # verbose
```

## Notes & limitations

- **Bulk indexing is not implemented.** `BULK_INDEX` must be present in the env
  (the loader requires it) but should be left `false`; if set, the connector
  warns and falls back to single-document indexing. The bulk code paths in
  `indexer.py` are commented out and would need finishing before use.
- The crawl is **full** every run — it re-indexes all assessments.
- OneTrust access tokens are refreshed automatically during long crawls
  (roughly every 30 minutes).

## Troubleshooting

- `key <NAME> NOT in environment` → a required var is missing from `.env`.
- `401`/auth errors from OneTrust → check `ONETRUST_CLIENT_ID` /
  `ONETRUST_CLIENT_SECRET` and `ONETRUST_BASE_URL`.
- Glean `ApiException` → verify `GLEAN_INSTANCE` and `GLEAN_PUSH_API_TOKEN`
  (and that the token has the Indexing scope).
- Nothing indexed → run with `DEBUG=true` to inspect the generated requests.
