import argparse
import asyncio
import pandas as pd
from .orchestrator import run_pipeline
from ipeds_crawler.logging import setup_logging


def main() -> None:
    setup_logging("INFO")
    parser = argparse.ArgumentParser(description="Run IPEDS crawler.")
    parser.add_argument("--input", required=True, help="Path to IPEDS HD CSV (with INSTNM, UNITID).")
    parser.add_argument("--output", required=True, help="Path to output CSV (append mode).")
    parser.add_argument("--min-year", type=int, default=2014, help="Minimum year, default=2014.")
    parser.add_argument("--max-year", type=int, default=2023, help="Maximum year, default=2023.")
    args = parser.parse_args()

    df = pd.read_csv(args.input, usecols=["INSTNM", "UNITID"])
    asyncio.run(run_pipeline(input_df=df, output_path=args.output, min_year=args.min_year, max_year=args.max_year))


if __name__ == "__main__":
    main()