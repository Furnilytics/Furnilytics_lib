import argparse
import sys
import os

from .client import Client, ClientError


def main():
    p = argparse.ArgumentParser(prog="furnilytics", description="CLI for Furnilytics API")
    p.add_argument("--api-key", default=os.getenv("FURNILYTICS_API_KEY"), help="Optional (only needed for pro datasets)")
    p.add_argument("--base-url", default=os.getenv("FURNILYTICS_BASE_URL", "https://furnilytics-api.fly.dev"))

    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("health")

    sub.add_parser("datasets")
    sub.add_parser("metadata")

    s_meta = sub.add_parser("meta")
    s_meta.add_argument("id")

    s_data = sub.add_parser("data")
    s_data.add_argument("id")
    s_data.add_argument("--frm")
    s_data.add_argument("--to")
    s_data.add_argument("--limit", type=int, default=None)
    s_data.add_argument("--csv", help="Write result to CSV file")

    args = p.parse_args()

    cli = Client(api_key=args.api_key, base_url=args.base_url)

    try:
        if args.cmd == "health":
            print(cli.health())
        elif args.cmd == "datasets":
            df = cli.datasets()
            print(df.to_string(index=False))
        elif args.cmd == "metadata":
            df = cli.metadata()
            print(df.to_string(index=False))
        elif args.cmd == "meta":
            obj = cli.metadata_one(args.id)
            print(obj)
        elif args.cmd == "data":
            df = cli.data(args.id, frm=args.frm, to=args.to, limit=args.limit)
            if args.csv:
                df.to_csv(args.csv, index=False)
                print(f"Wrote {len(df)} rows to {args.csv}")
            else:
                print(df.to_string(index=False))
        else:
            p.error("Unknown command")

    except ClientError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()