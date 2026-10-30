# Extracting ATS Companies from Common Crawl (Powered by whirlwind-python)

This project uses techniques from [Common Crawl's `whirlwind-python`](https://github.com/commoncrawl/whirlwind-python) repository to extract companies that post jobs on major Applicant Tracking System (ATS) platforms.

---

## 1. How It Works

Common Crawl crawls billions of web pages every month and indexes them into:
1. **Columnar Parquet Index (`ccindex`)**: Stores structured metadata (`url`, `url_host_registered_domain`, `url_host_tld`, `warc_filename`, `warc_record_offset`, `warc_record_length`).
2. **WARC / WET / WAT Archives**: Compressed web page archives stored on S3.
3. **Web Graph (Host / Domain Level)**: Graphs of hostnames and domain connections across the web.

### Why `whirlwind-python` is the foundation:
- **`duck.py`**: Teaches how to query the Parquet index directly with DuckDB using predicate pushdown (`url_host_registered_domain` and `url_host_tld`) so only relevant row groups are accessed.
- **`warcio` Random Access**: Demonstrates how to use HTTP Range requests (`bytes={offset}-{offset+length}`) to pull a single 15KB page record from a 1GB WARC file in 50ms, allowing extraction of official company names from `<title>` tags and Schema.org `JobPosting` JSON-LD.

---

## 2. Major ATS Platforms Supported

| ATS Platform | Registered Domain | URL Pattern | Slug Type | Example URL |
| :--- | :--- | :--- | :--- | :--- |
| **Greenhouse** | `greenhouse.io` | `boards.greenhouse.io/{slug}` | Path | `boards.greenhouse.io/stripe` |
| **Lever** | `lever.co` | `jobs.lever.co/{slug}` | Path | `jobs.lever.co/netflix` |
| **Ashby** | `ashbyhq.com` | `jobs.ashbyhq.com/{slug}` | Path | `jobs.ashbyhq.com/openai` |
| **Workable** | `workable.com` | `apply.workable.com/{slug}` | Path | `apply.workable.com/spotify` |
| **SmartRecruiters** | `smartrecruiters.com` | `jobs.smartrecruiters.com/{slug}` | Path | `jobs.smartrecruiters.com/square` |
| **Workday** | `myworkdayjobs.com` | `{slug}.wd*.myworkdayjobs.com` | Subdomain | `salesforce.wd12.myworkdayjobs.com` |
| **BambooHR** | `bamboohr.com` | `{slug}.bamboohr.com/careers` | Subdomain | `acme.bamboohr.com/careers` |
| **Recruitee** | `recruitee.com` | `{slug}.recruitee.com` | Subdomain | `techcorp.recruitee.com` |
| **JazzHR** | `applytojob.com` | `{slug}.applytojob.com` | Subdomain | `innovate.applytojob.com` |
| **Jobvite** | `jobvite.com` | `jobs.jobvite.com/{slug}` | Path | `jobs.jobvite.com/universal` |
| **Rippling** | `rippling.com` | `ats.rippling.com/{slug}` | Path | `ats.rippling.com/startup` |
| **Pinpoint** | `pinpointhq.com` | `{slug}.pinpointhq.com` | Subdomain | `growth.pinpointhq.com` |
| **Breezy HR** | `breezy.hr` | `{slug}.breezy.hr` | Subdomain | `venture.breezy.hr` |

---

## 3. Project Structure

```
ATS_companies/
├── whirlwind-python/              # Cloned Common Crawl whirlwind tutorial repository
├── venv/                          # Python virtual environment (duckdb, warcio, requests)
├── ats_definitions.py             # ATS registry with domain patterns & regex slug parsers
├── query_ats_duckdb.py            # DuckDB SQL runner for Common Crawl Parquet index
├── extract_company_names_warc.py  # warcio random-access extractor for verified company names
├── athena_queries.sql             # AWS Athena production SQL query (scans full corpus in seconds)
└── README.md                      # Documentation & usage guide
```

---

## 4. How to Run

### Setup
Activate the virtual environment:
```bash
source venv/bin/activate
```

### Method A: Query Index with DuckDB (Local or Sample CloudFront)
To query partitions directly using DuckDB:
```bash
# Query the first 5 parquet partitions over CloudFront:
python query_ats_duckdb.py --crawl CC-MAIN-2024-22 --partitions 5 --output ats_sample

# Or query locally downloaded parquet files:
python query_ats_duckdb.py --mode local --local-dir /path/to/downloaded/crawl --output ats_full
```

### Method B: Extract Official Company Names using `warcio`
Once the index query produces sample WARC offsets, you can resolve the human-readable company name and job title from the raw WARC record without downloading the full archive:
```bash
python extract_company_names_warc.py \
  --filename "crawl-data/CC-MAIN-2024-22/segments/1715971057216.39/warc/CC-MAIN-20240517233122-20240518023122-00000.warc.gz" \
  --offset 80610731 \
  --length 17423
```
Output:
```json
{
  "company_name": "Biquipedia, a enciclopedia libre",
  "job_title": null,
  "page_title": "Escopete - Biquipedia, a enciclopedia libre",
  "target_uri": "https://an.wikipedia.org/wiki/Escopete"
}
```

### Method C: Run at Full Scale with AWS Athena (Zero Download)
If you want to extract **all** companies across all ATS platforms for an entire monthly crawl (over 3 billion pages) in ~20 seconds without downloading 300GB of index files:
1. Open the [AWS Athena Console](https://console.aws.amazon.com/athena/).
2. Run the SQL script in `athena_queries.sql`.
3. Export the resulting table to CSV or S3.

