# IPEDS-Crawler

**IPEDS-Crawler** is an asynchronous Python scraper for the [U.S. Department of Education’s NCES / IPEDS](https://nces.ed.gov/ipeds/) “Reported Data” pages.  
It automates collection of institutional-level statistics such as tuition, admissions, enrollment, completions, graduation rates, financial aid, finance, staffing, and library data across multiple academic years.

The project is built on modern Python tooling:
- [Playwright](https://playwright.dev/python/) for browser automation  
- [uv](https://docs.astral.sh/uv/) for reproducible dependency management  
- `asyncio` for concurrent I/O  
- `pandas` for structured output  
- Pre-commit, Ruff, and Mypy for style and type safety

---

## 📦 Installation

### 1. Clone the repository
```bash
git clone https://github.com/Sakocpo/ipeds-crawler.git
cd ipeds-crawler
```

### 2. Sync dependencies using **uv**
```bash
uv sync && uv lock
```

### 3. Install Playwright browsers
```bash
uv run playwright install
```

## 🚀 Usage

### Basic commands
```bash
uv run ipeds-crawler \
  --input data/input/HD2023_test.csv \
  --output data/output/output_test.csv \
  --min-year 2014 \
  --max-year 2023
```

## ⚙️ Command-Line Arguments

| Flag | Description |
|------|--------------|
| `--input` | Path to a CSV containing columns `INSTNM` (institution name) and `UNITID` (IPEDS ID). |
| `--output` | Path to the output CSV file. Each run appends results for new `(institution, year)` pairs. |
| `--min-year` | Starting academic year (inclusive). |
| `--max-year` | Ending academic year (inclusive). |

---

## 🧱 Project Structure

```
ipeds-crawler/
│
├── pyproject.toml              # uv + build configuration
├── uv.lock                     # dependency lockfile
├── src/ipeds_crawler/
│     ├── cli.py                # command-line entry point
│     ├── orchestrator.py       # main crawling logic
│     ├── extractors.py         # Playwright selectors and parsing
│     ├── normalize.py          # normalization utilities
│     ├── ipeds_pages.py        # navigation helpers
│     └── ...
├── data/
│     ├── input/                # input HD CSVs
│     └── output/               # crawler outputs
├── tests/                      # pytest tests
├── .pre-commit-config.yaml     # linting & type-check hooks
└── README.md
```

---

## 🙌 Acknowledgements

- U.S. Department of Education — [National Center for Education Statistics (NCES)](https://nces.ed.gov/)  
- [Playwright](https://playwright.dev/python/)  
- [uv](https://docs.astral.sh/uv/) by Astral  
- Python ≥ 3.10  

---