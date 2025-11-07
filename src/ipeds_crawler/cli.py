import argparse
import asyncio
import pandas as pd
from .orchestrator import run_pipeline


def main() -> None:
    """
    Simple CLI wrapper. Stays close to your original interface:
    - input CSV must contain INSTNM and UNITID columns.
    - output CSV will be appended to (header written if the file doesn't exist).
    - year range is fixed to 2023..2014 (inclusive) by default to match your script.
      You can override with --min-year / --max-year.
    """
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