# furnilytics

Client for the Furnilytics API with hierarchy **topic / subtopic / dataset**.


Authentication via header **X-API-KEY** (set `FURNILYTICS_API_KEY` env var or pass `api_key=`).
Documentation can be found on www.furnilytics.com or contact thijs.meijerink@furnilytics.com

## Install (editable for local dev)
```bash
pip install -e .
```

## Quick start
```python
import os
import pandas as pd
from furnilytics import Client

os.environ["FURNILYTICS_API_KEY"] = "your_key_here"

cli = Client()  # base_url is fixed to https://furnilytics-api.fly.dev

# Discover
topics_df = cli.list_topics()              # topic-level (one row per subtopic entry)
flat_df   = cli.list_datasets_flat()       # all datasets with topic/subtopic
print(topics_df.head()); print(flat_df.head())

# Inspect dataset
meta_df, columns_df = cli.dataset_info("macro_economics","ppi","woodbased_panels_ppi")
print(meta_df); print(columns_df)

# Query rows (DataFrame)
df = cli.get("macro_economics","ppi","woodbased_panels_ppi", limit=5, country="SE")
print(df)

# Select, order, date window (if dataset has time_column)
df2 = cli.get("macro_economics","ppi","woodbased_panels_ppi",
              select=["date","ppi","country"],
              order_by="date", order_dir="DESC",
              frm="2022-01-01", to="2023-12-31",
              country="SE", limit=100)
```