# Furnilytics Python Library

A lightweight, production-ready Python wrapper for the Furnilytics REST API.

The client abstracts HTTP handling, authentication, rate-limiting, and error management, returning datasets as Pandas DataFrames for immediate analytical use.

Supports:

* `/datasets` catalog discovery
* `/metadata` inspection
* `/data/{id}` retrieval
* Public and API-key protected datasets

Ideal for research automation, dashboards, and quantitative analysis pipelines. 

---

## Installation

```bash
pip install furnilytics
```

---

## Authentication

Public datasets can be accessed without an API key.

Pro datasets require an API key via the `X-API-Key` header.

### Option 1 — Environment Variable (recommended)

```bash
export FURNILYTICS_API_KEY="your_api_key"
```

On Windows PowerShell:

```powershell
$env:FURNILYTICS_API_KEY="your_api_key"
```

### Option 2 — Pass directly to Client

```python
from furnilytics import Client

cli = Client(api_key="your_api_key")
```

---

## Quick Start

```python
from furnilytics import Client

cli = Client()

# API health
print(cli.health())

# List available datasets
datasets = cli.datasets()
print(datasets.head())

# List metadata
meta = cli.metadata()
print(meta.head())

# Get metadata for one dataset
info = cli.metadata_one("macro_economics/consumer/eu_consumer_sentiment")
print(info["meta"])

# Fetch dataset rows
df = cli.data("macro_economics/consumer/eu_consumer_sentiment", limit=10)
print(df.head())
```

---

## Core Endpoints Covered

The client wraps the following API endpoints:

| Method             | Endpoint         | Description              |
| ------------------ | ---------------- | ------------------------ |
| `health()`         | `/health`        | API status check         |
| `datasets()`       | `/datasets`      | Dataset catalog          |
| `metadata()`       | `/metadata`      | Full metadata list       |
| `metadata_one(id)` | `/metadata/{id}` | Metadata for one dataset |
| `data(id)`         | `/data/{id}`     | Data rows only           |

Dataset IDs use the format:

```
topic/subtopic/table_id
```

Example:

```
macro_economics/prices/eu_hicp_energy
```

---

## Filtering Data

The `data()` method supports optional filters:

```python
df = cli.data(
    "macro_economics/consumer/eu_consumer_sentiment",
    frm="2020-01-01",
    to="2023-12-31",
    limit=1000
)
```

Parameters:

* `frm` — start date (YYYY-MM-DD)
* `to` — end date (YYYY-MM-DD)
* `limit` — max rows (server limit applies)

---

## Public vs Pro Datasets

Datasets include a `visibility` field:

* `"public"` — accessible without API key
* `"paid"` / `"pro"` — requires API key

If a Pro dataset is accessed without a key, the client raises:

```python
AuthError
```

Example:

```python
from furnilytics import Client, AuthError

cli = Client(api_key=None)

try:
    cli.data("some/pro/dataset")
except AuthError as e:
    print("Access denied:", e)
```

---

## Error Handling

The client raises structured exceptions:

| Exception        | Meaning                    |
| ---------------- | -------------------------- |
| `AuthError`      | Missing or invalid API key |
| `NotFoundError`  | Dataset not found          |
| `RateLimitError` | Too many requests          |
| `ClientError`    | Other 4xx or 5xx errors    |

Example:

```python
try:
    df = cli.data("unknown/id")
except NotFoundError:
    print("Dataset does not exist.")
```

---

## Debugging

You can inspect response metadata:

```python
cli.data("macro_economics/prices/eu_hicp_energy", limit=5)
print(cli.last_response_meta)
```

Includes:

* HTTP status
* ETag
* Cache headers
* Rate-limit headers (if present)

---

## Development

Clone the repository:

```bash
git clone https://github.com/Furnilytics/Furnilytics_lib.git
cd Furnilytics_lib
pip install -e .
```

Build locally:

```bash
python -m build
```

Publish to PyPI via tagged release:

```bash
git tag v0.2.x
git push origin v0.2.x
```

---

## Project Structure

```
furnilytics/
 ├── client.py
 ├── __main__.py
 └── __init__.py
```

---

## Requirements

* Python 3.9+
* requests
* pandas

---

## About Furnilytics

Furnilytics is a data-driven market intelligence initiative focused on the global furniture industry, integrating macroeconomic, trade, retail, and digital demand data into structured datasets and analytical indicators.

Website: [https://www.furnilytics.com](https://www.furnilytics.com)

---

## License

MIT License

---
