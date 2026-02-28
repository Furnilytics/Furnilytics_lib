"""
Furnilytics Python Client – Quickstart Example

Run:
    python examples_quickstart.py

Optional:
    export FURNILYTICS_API_KEY="your_key"   # for pro datasets
"""

from furnilytics import Client, AuthError, NotFoundError


def main():
    print("Connecting to Furnilytics API...\n")

    # Automatically reads FURNILYTICS_API_KEY if present
    cli = Client()

    # --------------------------------------------------
    # 1️⃣ Health check
    # --------------------------------------------------
    print("Health check:")
    print(cli.health())
    print("-" * 50)

    # --------------------------------------------------
    # 2️⃣ List datasets
    # --------------------------------------------------
    print("Available datasets (first 5):")
    datasets = cli.datasets()
    print(datasets.head())
    print("-" * 50)

    # --------------------------------------------------
    # 3️⃣ List metadata
    # --------------------------------------------------
    print("Metadata overview (first 5):")
    metadata = cli.metadata()
    print(metadata.head())
    print("-" * 50)

    # --------------------------------------------------
    # 4️⃣ Fetch a public dataset
    # --------------------------------------------------
    example_id = "macro_economics/prices/eu_hicp_energy"

    print(f"Fetching dataset: {example_id}")
    try:
        df = cli.data(example_id, limit=5)
        print(df)
    except NotFoundError:
        print("Dataset not found.")
    except AuthError as e:
        print("Authentication error:", e)

    print("-" * 50)

    # --------------------------------------------------
    # 5️⃣ Demonstrate Pro dataset handling (if available)
    # --------------------------------------------------
    paid = metadata[metadata["visibility"].isin(["paid", "pro"])]

    if not paid.empty:
        pro_id = paid["id"].iloc[0]
        print(f"Testing Pro dataset access: {pro_id}")

        try:
            df = cli.data(pro_id, limit=5)
            print(df.head())
        except AuthError:
            print("Pro dataset requires an API key.")
    else:
        print("No Pro datasets found in catalog.")


if __name__ == "__main__":
    main()